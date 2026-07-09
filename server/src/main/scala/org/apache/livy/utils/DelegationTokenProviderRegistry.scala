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

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.livy.{LivyConf, Logging}

/**
 * Resolves delegation token providers from built-ins, ServiceLoader, and livy.conf mappings.
 *
 * Additional providers can be registered without modifying Livy core:
 *
 * {{{
 * livy.impersonation.delegation-token.providers = mycloud=com.example.MyTokenProvider
 * livy.impersonation.delegation-token.services = hdfs,hive,mycloud
 * }}}
 */
object DelegationTokenProviderRegistry extends Logging {

  private val builtInProviders: Seq[DelegationTokenProvider] = Seq(
    new FileSystemDelegationTokenProvider,
    new HiveDelegationTokenProvider,
    new HBaseDelegationTokenProvider,
    new KafkaDelegationTokenProvider)

  def providersFor(livyConf: LivyConf): Map[String, DelegationTokenProvider] = {
    val registry = mutable.LinkedHashMap[String, DelegationTokenProvider]()
    builtInProviders.foreach(p => registry(p.name) = p)
    loadServiceLoaderProviders(registry)
    loadConfiguredProviders(livyConf, registry)
    registry.toMap
  }

  def enabledProviderNames(livyConf: LivyConf): Set[String] = {
    Option(livyConf.get(LivyConf.DELEGATION_TOKEN_SERVICES))
      .map(_.split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSet)
      .getOrElse(Set("hdfs"))
  }

  def obtainEnabledTokens(
      proxyUser: String,
      proxyUgi: org.apache.hadoop.security.UserGroupInformation,
      renewer: String,
      livyConf: LivyConf,
      credentials: org.apache.hadoop.security.Credentials,
      sessionFilesystemUris: Set[String] = Set.empty): Unit = {
    val context = DelegationTokenProviderContext(
      proxyUser, proxyUgi, renewer, livyConf.hadoopConf, livyConf, sessionFilesystemUris)
    val providers = providersFor(livyConf)
    enabledProviderNames(livyConf).foreach { service =>
      providers.get(service) match {
        case Some(provider) =>
          try {
            provider.obtainTokens(context, credentials)
          } catch {
            case NonFatal(e) =>
              warn(s"Delegation token provider '$service' failed", e)
          }
        case None =>
          warn(s"No delegation token provider registered for service '$service'")
      }
    }
  }

  private def loadServiceLoaderProviders(
      registry: mutable.Map[String, DelegationTokenProvider]): Unit = {
    try {
      ServiceLoader.load(classOf[DelegationTokenProvider]).iterator().asScala.foreach { provider =>
        debug(s"Loaded delegation token provider '${provider.name}' from ServiceLoader")
        registry(provider.name) = provider
      }
    } catch {
      case NonFatal(e) =>
        warn("Failed to load delegation token providers from ServiceLoader", e)
    }
  }

  private def loadConfiguredProviders(
      livyConf: LivyConf,
      registry: mutable.Map[String, DelegationTokenProvider]): Unit = {
    configuredProviderEntries(livyConf).foreach { case (name, className) =>
      try {
        val clazz = Thread.currentThread().getContextClassLoader.loadClass(className)
        val provider = clazz.getConstructor().newInstance().asInstanceOf[DelegationTokenProvider]
        if (provider.name != name) {
          warn(s"Delegation token provider class $className reports name '${provider.name}' " +
            s"but config maps it as '$name'; using config name")
        }
        registry(name) = provider
        debug(s"Registered delegation token provider '$name' -> $className")
      } catch {
        case NonFatal(e) =>
          warn(s"Failed to load delegation token provider '$name' ($className)", e)
      }
    }
  }

  private[utils] def configuredProviderEntries(livyConf: LivyConf): Seq[(String, String)] = {
    Option(livyConf.get(LivyConf.DELEGATION_TOKEN_PROVIDERS))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq.empty)
      .flatMap { entry =>
        val parts = entry.split("=", 2).map(_.trim)
        if (parts.length == 2 && parts(0).nonEmpty && parts(1).nonEmpty) {
          Some(parts(0).toLowerCase -> parts(1))
        } else {
          warn(s"Ignoring invalid delegation token provider entry '$entry'; expected type=class")
          None
        }
      }
  }
}
