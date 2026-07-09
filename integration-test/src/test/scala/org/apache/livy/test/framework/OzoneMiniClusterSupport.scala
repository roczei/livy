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
import java.lang.reflect.InvocationTargetException

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.livy.Logging

/**
 * Embeds Apache Ozone [[MiniOzoneCluster]] into the Livy Kerberos mini cluster.
 * Ozone test dependencies are declared in integration-test/pom.xml (test scope).
 */
private[framework] object OzoneMiniClusterSupport extends MiniClusterUtils with Logging {

  val OZONE_ENABLED_KEY = "ozone.enabled"
  val OZONE_STARTED_KEY = "ozone.started"
  val OZONE_TEST_PATH_KEY = "livy.test.ozone.path"

  private val DEFAULT_SERVICE_ID = "omService"
  private val DEFAULT_VOLUME = "livy-it"
  private val DEFAULT_BUCKET = "bucket1"
  private val OzoneTokenMaxLifetime = "15s"
  private val OzoneTokenRenewInterval = "8s"
  private val OzoneTokenRemoverScanInterval = "5s"

  case class OzoneClusterInfo(
      cluster: AnyRef,
      serviceId: String,
      testPath: String,
      classpathJars: Seq[String])

  def isOzoneRequested(config: Map[String, String]): Boolean = {
    config.get(OZONE_ENABLED_KEY).forall(_.toBoolean)
  }

  def isMiniOzoneAvailable: Boolean = {
    try {
      Class.forName("org.apache.hadoop.ozone.MiniOzoneCluster")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  /** Ozone / HDDS jars from the Maven test classpath for Livy and Spark child processes. */
  def ozoneRuntimeJars: Seq[String] = {
    sys.props("java.class.path").split(File.pathSeparator)
      .filter { path =>
        val name = new File(path).getName.toLowerCase
        name.contains("ozone") || name.contains("hdds")
      }
      .distinct
  }

  def start(
      workDir: File,
      configDir: File,
      kerberosInfo: Option[KerberosMiniClusterSupport.KerberosClusterInfo],
      config: Map[String, String]): Option[OzoneClusterInfo] = {
    if (!isOzoneRequested(config)) {
      return None
    }

    if (!isMiniOzoneAvailable) {
      warn("Ozone is enabled but MiniOzoneCluster is not on the test classpath.")
      return None
    }

    try {
      val ozoneWorkDir = new File(workDir, "ozone")
      if (!ozoneWorkDir.mkdirs() && !ozoneWorkDir.isDirectory) {
        throw new IllegalStateException(s"Cannot create Ozone work dir $ozoneWorkDir")
      }

      val volume = config.getOrElse("ozone.test.volume", DEFAULT_VOLUME)
      val bucket = config.getOrElse("ozone.test.bucket", DEFAULT_BUCKET)
      val secureRequested = kerberosInfo.isDefined &&
        config.get("ozone.security.enabled").forall(_.toBoolean)

      def startCluster(secure: Boolean): AnyRef = {
        val conf = newOzoneConfiguration()
        kerberosInfo.foreach(info => configureSecureOzone(conf, info, secure))
        applyDelegationTokenTestSettings(conf)
        val cluster = buildMiniOzoneCluster(conf)
        waitForCluster(cluster)
        cluster
      }

      val cluster = try {
        startCluster(secureRequested)
      } catch {
        case NonFatal(e) if secureRequested =>
          warn("Secure MiniOzoneCluster failed; retrying without Ozone Kerberos.", e)
          startCluster(secure = false)
      }

      bootstrapVolumeBucket(cluster, volume, bucket)

      val ozoneConf = getClusterConf(cluster)
      saveConfig(ozoneConf, new File(configDir, "ozone-site.xml"))
      mergeOzoneIntoCoreSite(configDir, ozoneConf)

      val serviceId = Option(ozoneConf.get("ozone.om.service.ids"))
        .map(_.split(",").head.trim)
        .filter(_.nonEmpty)
        .getOrElse(DEFAULT_SERVICE_ID)
      val testPath = s"ofs://$serviceId/$volume/$bucket"

      info(s"MiniOzoneCluster ready at $testPath")
      Some(OzoneClusterInfo(cluster, serviceId, testPath, ozoneRuntimeJars))
    } catch {
      case NonFatal(e) =>
        warn("Failed to start embedded MiniOzoneCluster; Ozone IT will be skipped.", e)
        None
    }
  }

  def stop(info: Option[OzoneClusterInfo]): Unit = {
    info.foreach { i =>
      try {
        i.cluster.getClass.getMethod("shutdown").invoke(i.cluster)
      } catch {
        case NonFatal(e) =>
          warn("Failed to shut down MiniOzoneCluster", e)
      }
    }
  }

  def clusterProperties(info: OzoneClusterInfo): Map[String, String] = Map(
    OZONE_ENABLED_KEY -> "true",
    OZONE_STARTED_KEY -> "true",
    OZONE_TEST_PATH_KEY -> info.testPath,
    "ozone.om.service.ids" -> info.serviceId)

  private def newOzoneConfiguration(): Configuration = {
    val clazz = Class.forName("org.apache.hadoop.ozone.OzoneConfiguration")
    clazz.getConstructor().newInstance().asInstanceOf[Configuration]
  }

  private def configureSecureOzone(
      conf: Configuration,
      info: KerberosMiniClusterSupport.KerberosClusterInfo,
      enabled: Boolean): Unit = {
    if (!enabled) {
      conf.setBoolean("ozone.security.enabled", false)
      return
    }

    val hostRealm = s"${info.krbInstance}@${info.realm}"
    val keytab = info.keytabFile.getAbsolutePath
    conf.set("hadoop.security.authentication", "kerberos")
    conf.setBoolean("ozone.security.enabled", true)
    conf.set("ozone.om.kerberos.principal", s"om/$hostRealm")
    conf.set("ozone.om.kerberos.keytab.file", keytab)
    conf.set("hdds.scm.kerberos.principal", s"scm/$hostRealm")
    conf.set("hdds.scm.kerberos.keytab.file", keytab)
    conf.set("hdds.datanode.kerberos.principal", s"hdfs/$hostRealm")
    conf.set("hdds.datanode.kerberos.keytab.file", keytab)
    conf.set("ozone.administrators", info.livyPrincipal)
  }

  private def applyDelegationTokenTestSettings(conf: Configuration): Unit = {
    conf.set("ozone.manager.delegation.token.max-lifetime", OzoneTokenMaxLifetime)
    conf.set("ozone.manager.delegation.token.renew-interval", OzoneTokenRenewInterval)
    conf.set(
      "ozone.manager.delegation.remover.scan.interval", OzoneTokenRemoverScanInterval)
    // Keep secret key lifetime above (max-lifetime + rotate + remover scan) for short IT values.
    conf.set("hdds.secret.key.rotate.duration", "5s")
    conf.set("hdds.secret.key.expiry.duration", "1m")
  }

  private def buildMiniOzoneCluster(conf: Configuration): AnyRef = {
    val miniOzoneClass = Class.forName("org.apache.hadoop.ozone.MiniOzoneCluster")
    val builder = miniOzoneClass.getMethod("newBuilder", classOf[Configuration]).invoke(null, conf)
    builder.getClass.getMethod("build").invoke(builder)
  }

  private def waitForCluster(cluster: AnyRef): Unit = {
    cluster.getClass.getMethod("waitForClusterToBeReady").invoke(cluster)
  }

  private def getClusterConf(cluster: AnyRef): Configuration = {
    val methods = cluster.getClass.getMethods.filter(m =>
      m.getName == "getConf" && m.getParameterCount == 0)
    if (methods.nonEmpty) {
      methods.head.invoke(cluster).asInstanceOf[Configuration]
    } else {
      newOzoneConfiguration()
    }
  }

  private def bootstrapVolumeBucket(cluster: AnyRef, volume: String, bucket: String): Unit = {
    val client = cluster.getClass.getMethod("newClient").invoke(cluster)
    try {
      val store = client.getClass.getMethod("getObjectStore").invoke(client)
      invokeOptional(store, "createVolume", volume)
      val vol = store.getClass.getMethod("getVolume", classOf[String]).invoke(store, volume)
      invokeOptional(vol, "createBucket", bucket)
    } finally {
      client.getClass.getMethod("close").invoke(client)
    }
  }

  private def invokeOptional(target: AnyRef, methodName: String, arg: String): Unit = {
    try {
      target.getClass.getMethod(methodName, classOf[String]).invoke(target, arg)
    } catch {
      case e: InvocationTargetException if isAlreadyExists(e.getCause) =>
        debug(s"Ignoring existing Ozone object for $methodName($arg)")
      case NonFatal(e) =>
        debug(s"Optional Ozone call $methodName($arg) failed: ${e.getMessage}")
    }
  }

  private def isAlreadyExists(cause: Throwable): Boolean = {
    cause != null && (
      cause.getClass.getName.contains("AlreadyExists") ||
        Option(cause.getMessage).exists(_.toLowerCase.contains("already exist")))
  }

  private def mergeOzoneIntoCoreSite(configDir: File, ozoneConf: Configuration): Unit = {
    val coreFile = new File(configDir, "core-site.xml")
    val coreConf = new Configuration(false)
    if (coreFile.isFile) {
      coreConf.addResource(new org.apache.hadoop.fs.Path(coreFile.toURI))
    }
    Seq(
      "ozone.om.service.ids",
      "ozone.om.address",
      "ozone.scm.names",
      "ozone.scm.client.address",
      "ozone.security.enabled",
      "fs.ofs.impl",
      "fs.AbstractFileSystem.ofs.impl",
      "ozone.manager.delegation.token.max-lifetime",
      "ozone.manager.delegation.token.renew-interval",
      "ozone.manager.delegation.remover.scan.interval").foreach { key =>
      Option(ozoneConf.get(key)).foreach(coreConf.set(key, _))
    }
    saveConfig(coreConf, coreFile)
  }
}
