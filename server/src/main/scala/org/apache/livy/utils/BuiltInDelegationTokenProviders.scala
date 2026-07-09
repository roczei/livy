/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.utils

import java.lang.reflect.Method
import java.security.PrivilegedExceptionAction

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.livy.{LivyConf, Logging}

private[utils] object DelegationTokenProviderSupport extends Logging {

  def invokeOptionalProvider(
      className: String,
      methodName: String,
      proxyUser: String,
      renewer: String,
      conf: Configuration,
      creds: Credentials): Unit = {
    try {
      val clazz = Class.forName(className)
      val methods = clazz.getMethods.filter(_.getName == methodName)
      if (methods.isEmpty) {
        debug(s"No suitable $methodName method found on $className")
        return
      }
      methods.find(_.getParameterCount >= 2).foreach { method =>
        val args = buildArgs(method, proxyUser, renewer, conf, creds)
        method.invoke(null, args: _*)
        debug(s"Obtained delegation tokens via $className.$methodName")
      }
    } catch {
      case _: ClassNotFoundException =>
        debug(s"Optional token provider $className is not on the classpath")
      case NonFatal(e) =>
        warn(s"Failed to obtain delegation tokens via $className", e)
    }
  }

  private def buildArgs(
      method: Method,
      proxyUser: String,
      renewer: String,
      conf: Configuration,
      creds: Credentials): Array[AnyRef] = {
    method.getParameterTypes.map {
      case c if c == classOf[String] && method.getParameterCount == 2 => proxyUser: AnyRef
      case c if c == classOf[String] => renewer: AnyRef
      case c if c == classOf[Configuration] => conf: AnyRef
      case c if c == classOf[Credentials] => creds: AnyRef
      case c if c == classOf[UserGroupInformation] =>
        UserGroupInformation.createProxyUser(
          proxyUser, UserGroupInformation.getLoginUser): AnyRef
      case _ => null
    }
  }
}

/** Obtains filesystem delegation tokens for fs.defaultFS, extra filesystem URIs, and session URIs.
  * Covers HDFS (hdfs://), Ozone (ofs://, o3fs://), and other Hadoop FileSystem implementations
  * that support addDelegationTokens().
  */
class FileSystemDelegationTokenProvider extends DelegationTokenProvider with Logging {
  override val name: String = "hdfs"

  override def obtainTokens(
      context: DelegationTokenProviderContext,
      credentials: Credentials): Unit = {
    obtainTokensForUris(context, filesystemUris(context), credentials)
  }

  private[utils] def obtainTokensForUris(
      context: DelegationTokenProviderContext,
      uris: Set[String],
      credentials: Credentials): Unit = {
    uris.foreach { uri =>
      try {
        context.proxyUgi.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = {
            val fs = FileSystem.get(new java.net.URI(uri), context.hadoopConf)
            try {
              val tokens = fs.addDelegationTokens(context.renewer, credentials)
              if (tokens != null) {
                debug(s"Obtained filesystem delegation tokens from $uri")
              }
            } finally {
              fs.close()
            }
          }
        })
      } catch {
        case NonFatal(e) =>
          warn(s"Failed to obtain delegation tokens from filesystem $uri", e)
      }
    }
  }

  private def filesystemUris(context: DelegationTokenProviderContext): Set[String] = {
    val configured = Option(context.hadoopConf.get("fs.defaultFS")) ++
      Option(context.livyConf.get(LivyConf.DELEGATION_TOKEN_EXTRA_FILESYSTEMS))
        .map(_.split(",").map(_.trim).filter(_.nonEmpty))
        .getOrElse(Array.empty)
    (configured ++ context.sessionFilesystemUris).filter(_.nonEmpty).toSet
  }
}

class HiveDelegationTokenProvider extends DelegationTokenProvider with Logging {
  override val name: String = "hive"

  override def obtainTokens(
      context: DelegationTokenProviderContext,
      credentials: Credentials): Unit = {
    try {
      val hiveConfClass = Class.forName("org.apache.hadoop.hive.conf.HiveConf")
      val hiveConf = hiveConfClass.getConstructor(classOf[Configuration])
        .newInstance(context.hadoopConf)
      val clientClass = Class.forName("org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
      val client = clientClass.getConstructor(hiveConfClass)
        .newInstance(hiveConf.asInstanceOf[Object])
      try {
        val tokenStr = clientClass
          .getMethod("getDelegationToken", classOf[String], classOf[String])
          .invoke(client, context.proxyUser, context.renewer)
          .asInstanceOf[String]
        if (tokenStr != null && tokenStr.nonEmpty) {
          val token = new Token[TokenIdentifier]()
          token.decodeFromUrlString(tokenStr)
          credentials.addToken(new Text("hive.server.delegation.token"), token)
          debug("Obtained Hive metastore delegation token")
        }
      } finally {
        clientClass.getMethod("close").invoke(client)
      }
    } catch {
      case _: ClassNotFoundException =>
        debug("Hive metastore client is not on the classpath")
      case NonFatal(e) =>
        warn("Failed to obtain Hive metastore delegation token", e)
    }
  }
}

class HBaseDelegationTokenProvider extends DelegationTokenProvider {
  override val name: String = "hbase"

  override def obtainTokens(
      context: DelegationTokenProviderContext,
      credentials: Credentials): Unit = {
    DelegationTokenProviderSupport.invokeOptionalProvider(
      "org.apache.hadoop.hbase.security.token.TokenUtil",
      "obtainDelegationToken",
      context.proxyUser,
      context.renewer,
      context.hadoopConf,
      credentials)
  }
}

class KafkaDelegationTokenProvider extends DelegationTokenProvider {
  override val name: String = "kafka"

  override def obtainTokens(
      context: DelegationTokenProviderContext,
      credentials: Credentials): Unit = {
    DelegationTokenProviderSupport.invokeOptionalProvider(
      "org.apache.kafka.common.security.token.delegation.DelegationTokenManager",
      "obtainDelegationTokens",
      context.proxyUser,
      context.renewer,
      context.hadoopConf,
      credentials)
  }
}
