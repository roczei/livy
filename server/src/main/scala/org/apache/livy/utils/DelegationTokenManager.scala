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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, File}
import java.security.PrivilegedExceptionAction
import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.livy.{LivyConf, Logging}

/**
 * Obtains and renews Hadoop delegation tokens for proxy-user sessions.
 *
 * Token acquisition is delegated to [[DelegationTokenProvider]] implementations resolved by
 * [[DelegationTokenProviderRegistry]]. Renewal still uses the Hadoop TokenRenewer SPI.
 */
object DelegationTokenManager extends Logging {

  private val HdfsDelegationTokenMaxLifetimeKey =
    "dfs.namenode.delegation.token.max-lifetime"
  private val HiveDelegationTokenMaxLifetimeKey =
    "hive.cluster.delegation.token.max-lifetime"
  private val HbaseAuthTokenMaxLifetimeKey = "hbase.auth.token.max.lifetime"
  private val OzoneDelegationTokenMaxLifetimeKey =
    "ozone.manager.delegation.token.max-lifetime"
  private val KafkaDelegationTokenMaxLifetimeKey = "delegation.token.max.lifetime.ms"

  val SPARK_EXTRA_LISTENERS_KEY = "spark.extraListeners"
  val DELEGATION_TOKEN_RPC_BOOTSTRAP_CLASS =
    "org.apache.livy.rsc.driver.DelegationTokenRpcBootstrap"

  def isEnabled(livyConf: LivyConf, proxyUser: Option[String]): Boolean = {
    proxyUser.isDefined &&
      UserGroupInformation.isSecurityEnabled &&
      livyConf.getBoolean(LivyConf.DELEGATION_TOKEN_RENEWAL_ENABLED) &&
      livyConf.getBoolean(LivyConf.IMPERSONATION_ENABLED)
  }

  def renewerPrincipal(livyConf: LivyConf): String = {
    Option(livyConf.get(LivyConf.DELEGATION_TOKEN_RENEWER_PRINCIPAL))
      .orElse(Option(livyConf.get(LivyConf.LAUNCH_KERBEROS_PRINCIPAL)))
      .map(SecurityUtil.getServerPrincipal(_, "0.0.0.0"))
      .getOrElse(UserGroupInformation.getCurrentUser.getShortUserName)
  }

  def obtainTokens(
      proxyUser: String,
      livyConf: LivyConf,
      sessionFilesystemUris: Set[String] = Set.empty): Credentials = {
    val realUser = UserGroupInformation.getLoginUser
    val proxyUgi = UserGroupInformation.createProxyUser(proxyUser, realUser)
    val renewer = renewerPrincipal(livyConf)

    realUser.doAs(new PrivilegedExceptionAction[Credentials] {
      override def run(): Credentials = {
        val creds = new Credentials()
        DelegationTokenProviderRegistry.obtainEnabledTokens(
          proxyUser, proxyUgi, renewer, livyConf, creds, sessionFilesystemUris)
        creds
      }
    })
  }

  /** Re-obtain filesystem delegation tokens for session-discovered URIs during renewal. */
  def refreshSessionFilesystemTokens(
      proxyUser: String,
      livyConf: LivyConf,
      sessionFilesystemUris: Set[String],
      credentials: Credentials): Unit = {
    if (sessionFilesystemUris.isEmpty) {
      return
    }
    if (!DelegationTokenProviderRegistry.enabledProviderNames(livyConf).contains("hdfs")) {
      return
    }

    val realUser = UserGroupInformation.getLoginUser
    val proxyUgi = UserGroupInformation.createProxyUser(proxyUser, realUser)
    val renewer = renewerPrincipal(livyConf)
    val context = DelegationTokenProviderContext(
      proxyUser, proxyUgi, renewer, livyConf.hadoopConf, livyConf, sessionFilesystemUris)

    realUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        new FileSystemDelegationTokenProvider()
          .obtainTokensForUris(context, sessionFilesystemUris, credentials)
      }
    })
  }

  def renewTokens(credentials: Credentials, livyConf: LivyConf): Unit = {
    val conf = livyConf.hadoopConf
    val realUser = UserGroupInformation.getLoginUser
    realUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        val renewers = loadTokenRenewers()
        credentials.getAllTokens.asScala.foreach { token =>
          val renewer = renewers.find(_.handleKind(token.getKind))
          renewer.foreach { r =>
            try {
              if (r.isManaged(token)) {
                val newExpiry = r.renew(token, conf)
                debug(s"Renewed token kind ${token.getKind} until $newExpiry")
              }
            } catch {
              case NonFatal(e) =>
                warn(s"Failed to renew token kind ${token.getKind}", e)
            }
          }
        }
      }
    })
  }

  def nextRenewalTime(credentials: Credentials, livyConf: LivyConf): Option[Long] = {
    val renewers = loadTokenRenewers()
    val intervals = credentials.getAllTokens.asScala.flatMap { token =>
      renewers.find(_.handleKind(token.getKind)).flatMap { renewer =>
        try {
          if (renewer.isManaged(token)) {
            val maxLifetime = getMaxLifetime(token, livyConf.hadoopConf)
            val interval = math.max(0L, maxLifetime) / 2
            Some(interval)
          } else {
            None
          }
        } catch {
          case NonFatal(_) => None
        }
      }
    }
    if (intervals.isEmpty) None else Some(intervals.min)
  }

  def serialize(credentials: Credentials): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    credentials.writeTokenStorageToStream(new DataOutputStream(out))
    out.toByteArray
  }

  def deserialize(bytes: Array[Byte]): Credentials = {
    val creds = new Credentials()
    creds.readTokenStorageStream(new DataInputStream(new ByteArrayInputStream(bytes)))
    creds
  }

  /** RSC jars required on the Spark driver classpath for batch delegation token RPC. */
  private[utils] def rscJarPaths(livyConf: LivyConf): Option[String] = {
    val jars = Option(livyConf.get(LivyConf.RSC_JARS))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty))
      .filter(_.nonEmpty)
      .getOrElse {
        sys.env.get("LIVY_HOME").map { home =>
          val candidates = Seq(
            new File(home, "rsc-jars"),
            new File(home, "rsc/target/jars"))
          val dir = candidates.find(_.isDirectory).getOrElse(return None)
          dir.listFiles().filter(_.getName.endsWith(".jar")).map(_.getAbsolutePath)
        }.getOrElse(Array.empty)
      }
    if (jars.isEmpty) None else Some(jars.mkString(","))
  }

  private def loadTokenRenewers(): Seq[org.apache.hadoop.security.token.TokenRenewer] = {
    ServiceLoader.load(classOf[org.apache.hadoop.security.token.TokenRenewer]).asScala.toSeq
  }

  private def getMaxLifetime(
      token: Token[_ <: TokenIdentifier],
      conf: Configuration): Long = {
    val defaultMaxLifetime = 7L * 24 * 3600 * 1000
    token.getKind.toString match {
      case kind if kind.contains("HDFS_DELEGATION") =>
        conf.getLong(HdfsDelegationTokenMaxLifetimeKey, defaultMaxLifetime)
      case kind if kind.contains("HIVE_DELEGATION") =>
        conf.getLong(HiveDelegationTokenMaxLifetimeKey, defaultMaxLifetime)
      case kind if kind.contains("HBASE") =>
        conf.getLong(HbaseAuthTokenMaxLifetimeKey, defaultMaxLifetime)
      case kind if kind.contains("OzoneToken") || kind.toUpperCase.contains("OZONE") =>
        getConfigDurationMs(conf, OzoneDelegationTokenMaxLifetimeKey, defaultMaxLifetime)
      case kind if kind.contains("KAFKA") =>
        conf.getLong(KafkaDelegationTokenMaxLifetimeKey, defaultMaxLifetime)
      case _ =>
        24L * 3600 * 1000
    }
  }

  private def getConfigDurationMs(
      conf: Configuration,
      key: String,
      defaultMs: Long): Long = {
    Option(conf.get(key)).map(_.trim).filter(_.nonEmpty) match {
      case None => conf.getLong(key, defaultMs)
      case Some(value) if value.forall(Character.isDigit) => value.toLong
      case Some(value) if value.length >= 2 =>
        val unit = value.takeRight(1).toLowerCase
        val amount = value.dropRight(1).toLong
        unit match {
          case "s" => amount * 1000L
          case "m" => amount * 60L * 1000L
          case "h" => amount * 3600L * 1000L
          case "d" => amount * 24L * 3600L * 1000L
          case _ => conf.getLong(key, defaultMs)
        }
      case Some(_) => conf.getLong(key, defaultMs)
    }
  }
}
