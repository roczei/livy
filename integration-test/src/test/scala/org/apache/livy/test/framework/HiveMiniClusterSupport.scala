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
 * Embeds a standalone Hive Metastore into the Kerberos mini cluster (Maven test scope).
 */
private[framework] object HiveMiniClusterSupport extends MiniClusterUtils with Logging {

  val HIVE_ENABLED_KEY = "hive.enabled"
  val HIVE_STARTED_KEY = "hive.started"

  case class HiveClusterInfo(
      metastoreUri: String,
      classpathJars: Seq[String])

  def isHiveRequested(config: Map[String, String]): Boolean = {
    EmbeddedServiceSupport.isServiceRequested(config, HIVE_ENABLED_KEY)
  }

  def isHiveMetastoreAvailable: Boolean = {
    EmbeddedServiceSupport.isClassAvailable("org.apache.hadoop.hive.metastore.HiveMetaStore")
  }

  def start(
      workDir: File,
      configDir: File,
      kerberosInfo: Option[KerberosMiniClusterSupport.KerberosClusterInfo],
      config: Map[String, String]): Option[HiveClusterInfo] = {
    if (!isHiveRequested(config)) {
      return None
    }
    if (!isHiveMetastoreAvailable) {
      warn("Hive is enabled but HiveMetaStore is not on the test classpath.")
      return None
    }

    try {
      val hiveWorkDir = new File(workDir, "hive")
      if (!hiveWorkDir.mkdirs() && !hiveWorkDir.isDirectory) {
        throw new IllegalStateException(s"Cannot create Hive work dir $hiveWorkDir")
      }

      val port = EmbeddedServiceSupport.findFreePort()
      val metastoreUri = s"thrift://localhost:$port"
      val derbyDb = new File(hiveWorkDir, "metastore_db").getAbsolutePath

      val coreConf = new Configuration(false)
      val coreFile = new File(configDir, "core-site.xml")
      if (coreFile.isFile) {
        coreConf.addResource(new Path(coreFile.toURI))
      }

      val hiveConf = newHiveConf(coreConf)
      hiveConf.set("hive.metastore.uris", metastoreUri)
      hiveConf.set("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$derbyDb;create=true")
      hiveConf.set("javax.jdo.option.ConnectionDriverName",
        "org.apache.derby.jdbc.EmbeddedDriver")
      hiveConf.set("datanucleus.schema.autoCreateTables", "true")
      hiveConf.set("hive.metastore.schema.verification", "false")
      hiveConf.set("metastore.warehouse.dir", new File(hiveWorkDir, "warehouse").toURI.toString)

      kerberosInfo.foreach(info => configureSecureHive(hiveConf, info))

      saveConfig(hiveConf, new File(configDir, "hive-site.xml"))
      EmbeddedServiceSupport.mergeIntoCoreSite(configDir,
        Seq(
          "hive.metastore.uris",
          "hive.cluster.delegation.token.max-lifetime",
          "hive.cluster.delegation.token.renew-interval",
          "hive.cluster.delegation.token.gc-interval"),
        hiveConf)

      startMetastoreServer(port, hiveConf)

      info(s"Embedded Hive Metastore ready at $metastoreUri")
      Some(HiveClusterInfo(metastoreUri, EmbeddedServiceSupport.runtimeJars(Seq("hive"))))
    } catch {
      case NonFatal(e) =>
        warn("Failed to start embedded Hive Metastore; Hive IT will be skipped.", e)
        None
    }
  }

  def stop(): Unit = {
    try {
      val metastoreClass = Class.forName("org.apache.hadoop.hive.metastore.HiveMetaStore")
      metastoreClass.getMethods.find(_.getName == "stopMetaStore").foreach { method =>
        if (method.getParameterCount == 0) {
          method.invoke(null)
        }
      }
    } catch {
      case NonFatal(e) =>
        debug(s"Hive Metastore shutdown: ${e.getMessage}")
    }
  }

  def clusterProperties(info: HiveClusterInfo): Map[String, String] = Map(
    HIVE_ENABLED_KEY -> "true",
    HIVE_STARTED_KEY -> "true",
    "hive.metastore.uris" -> info.metastoreUri)

  private def newHiveConf(base: Configuration): Configuration = {
    val hiveConfClass = Class.forName("org.apache.hive.conf.HiveConf")
    hiveConfClass.getConstructor(classOf[Configuration], classOf[Class[_]])
      .newInstance(base, Class.forName("org.apache.hadoop.hive.conf.HiveConf"))
      .asInstanceOf[Configuration]
  }

  private def configureSecureHive(
      conf: Configuration,
      info: KerberosMiniClusterSupport.KerberosClusterInfo): Unit = {
    val hostRealm = s"${info.krbInstance}@${info.realm}"
    val keytab = info.keytabFile.getAbsolutePath
    conf.set("hive.metastore.sasl.enabled", "true")
    conf.set("hive.metastore.kerberos.principal", s"hive/$hostRealm")
    conf.set("hive.metastore.kerberos.keytab.file", keytab)
    conf.set("hive.metastore.execute.setugi", "true")
  }

  private def startMetastoreServer(port: Int, conf: Configuration): Unit = {
    val bridgeClass = Class.forName("org.apache.hadoop.hive.metastore.HadoopThriftAuthBridge")
    val bridge = bridgeClass.getMethod("getBridge").invoke(null)
    val metastoreClass = Class.forName("org.apache.hadoop.hive.metastore.HiveMetaStore")
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          metastoreClass.getMethod(
            "startMetaStore",
            classOf[Int],
            bridgeClass,
            classOf[Configuration]).invoke(null, Integer.valueOf(port), bridge, conf)
        } catch {
          case e: Exception =>
            error("Hive Metastore server failed", e)
        }
      }
    }, "livy-hive-metastore")
    thread.setDaemon(true)
    thread.start()
    Thread.sleep(3000)
  }
}
