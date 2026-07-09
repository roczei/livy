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

package org.apache.livy.test.framework

import java.io.File

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.livy.Logging

/**
 * Embeds a mini HBase cluster into the Kerberos mini cluster (Maven test scope).
 */
private[framework] object HBaseMiniClusterSupport extends MiniClusterUtils with Logging {

  val HBASE_ENABLED_KEY = "hbase.enabled"
  val HBASE_STARTED_KEY = "hbase.started"

  case class HBaseClusterInfo(
      zookeeperQuorum: String,
      utility: AnyRef,
      classpathJars: Seq[String])

  def isHBaseRequested(config: Map[String, String]): Boolean = {
    EmbeddedServiceSupport.isServiceRequested(config, HBASE_ENABLED_KEY)
  }

  def isHBaseTestingUtilityAvailable: Boolean = {
    EmbeddedServiceSupport.isClassAvailable("org.apache.hadoop.hbase.HBaseTestingUtility") &&
      EmbeddedServiceSupport.isClassAvailable("org.apache.hadoop.hbase.HBaseZKTestingUtility")
  }

  def start(
      workDir: File,
      configDir: File,
      kerberosInfo: Option[KerberosMiniClusterSupport.KerberosClusterInfo],
      config: Map[String, String]): Option[HBaseClusterInfo] = {
    if (!isHBaseRequested(config)) {
      return None
    }
    if (!isHBaseTestingUtilityAvailable) {
      warn("HBase is enabled but HBaseTestingUtility is not on the test classpath.")
      return None
    }

    try {
      val hbaseWorkDir = new File(workDir, "hbase")
      if (!hbaseWorkDir.mkdirs() && !hbaseWorkDir.isDirectory) {
        throw new IllegalStateException(s"Cannot create HBase work dir $hbaseWorkDir")
      }

      val utilClass = Class.forName("org.apache.hadoop.hbase.HBaseTestingUtility")
      val util = utilClass.getConstructor().newInstance()
      val hbaseConf = utilClass.getMethod("getConfiguration").invoke(util).asInstanceOf[Configuration]
      hbaseConf.set("hbase.rootdir", new File(hbaseWorkDir, "root").toURI.toString)
      hbaseConf.set("hbase.zookeeper.property.dataDir", new File(hbaseWorkDir, "zk-data").toString)

      val secureRequested = kerberosInfo.isDefined
      if (secureRequested) {
        kerberosInfo.foreach(info => configureSecureHBase(hbaseConf, info))
        try {
          utilClass.getMethod("enableSecurity").invoke(util)
        } catch {
          case NonFatal(e) =>
            warn("HBase enableSecurity failed; continuing without HBase Kerberos.", e)
        }
      }

      utilClass.getMethod("startMiniCluster", classOf[Int]).invoke(util, Integer.valueOf(1))

      val clusterConf = utilClass.getMethod("getConfiguration").invoke(util).asInstanceOf[Configuration]
      saveConfig(clusterConf, new File(configDir, "hbase-site.xml"))
      EmbeddedServiceSupport.mergeIntoCoreSite(configDir,
        Seq(
          "hbase.zookeeper.quorum",
          "hbase.zookeeper.property.clientPort",
          "hbase.security.authentication",
          "hbase.auth.token.max.lifetime",
          "hbase.auth.key.update.interval"),
        clusterConf)

      val zkQuorum = Option(clusterConf.get("hbase.zookeeper.quorum"))
        .filter(_.nonEmpty)
        .getOrElse("localhost")
      info(s"Embedded HBase mini cluster ready (ZK quorum=$zkQuorum)")
      Some(HBaseClusterInfo(
        zkQuorum,
        util.asInstanceOf[AnyRef],
        EmbeddedServiceSupport.runtimeJars(Seq("hbase"))))
    } catch {
      case e: LinkageError =>
        warn("Failed to start embedded HBase mini cluster; HBase IT will be skipped.", e)
        None
      case NonFatal(e) =>
        warn("Failed to start embedded HBase mini cluster; HBase IT will be skipped.", e)
        None
    }
  }

  def stop(info: Option[HBaseClusterInfo]): Unit = {
    info.foreach { i =>
      try {
        i.utility.getClass.getMethod("shutdownMiniCluster").invoke(i.utility)
      } catch {
        case NonFatal(e) =>
          warn("Failed to shut down HBase mini cluster", e)
      }
    }
  }

  def clusterProperties(info: HBaseClusterInfo): Map[String, String] = Map(
    HBASE_ENABLED_KEY -> "true",
    HBASE_STARTED_KEY -> "true",
    "hbase.zookeeper.quorum" -> info.zookeeperQuorum)

  private def configureSecureHBase(
      conf: Configuration,
      info: KerberosMiniClusterSupport.KerberosClusterInfo): Unit = {
    val hostRealm = s"${info.krbInstance}@${info.realm}"
    val keytab = info.keytabFile.getAbsolutePath
    conf.set("hadoop.security.authentication", "kerberos")
    conf.set("hbase.security.authentication", "kerberos")
    conf.set("hbase.security.authorization", "true")
    conf.set("hbase.master.kerberos.principal", s"hbase/$hostRealm")
    conf.set("hbase.master.keytab.file", keytab)
    conf.set("hbase.regionserver.kerberos.principal", s"hbase/$hostRealm")
    conf.set("hbase.regionserver.keytab.file", keytab)
  }
}
