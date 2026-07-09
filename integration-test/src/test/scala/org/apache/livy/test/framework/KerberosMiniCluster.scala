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

/**
 * Mini cluster with embedded MiniKdc for Kerberos delegation token tests.
 */
class KerberosMiniCluster(config: Map[String, String]) extends MiniCluster(config) {

  private var kerberosInfo: Option[KerberosMiniClusterSupport.KerberosClusterInfo] = None
  private var ozoneInfo: Option[OzoneMiniClusterSupport.OzoneClusterInfo] = None
  private var hiveInfo: Option[HiveMiniClusterSupport.HiveClusterInfo] = None
  private var hbaseInfo: Option[HBaseMiniClusterSupport.HBaseClusterInfo] = None
  private var kafkaInfo: Option[KafkaMiniClusterSupport.KafkaClusterInfo] = None

  override protected def extraClasspathJars: Seq[String] =
    (ozoneInfo.toSeq.flatMap(_.classpathJars) ++
      hiveInfo.toSeq.flatMap(_.classpathJars) ++
      hbaseInfo.toSeq.flatMap(_.classpathJars) ++
      kafkaInfo.toSeq.flatMap(_.classpathJars)).distinct

  override protected def configureClusterConfig(
      configDir: File,
      initialConfig: Map[String, String]): Map[String, String] = {
    if (!KerberosMiniClusterSupport.isKerberosEnabled(initialConfig)) {
      return super.configureClusterConfig(configDir, initialConfig)
    }

    kerberosInfo = Some(KerberosMiniClusterSupport.startKerberos(tempDir, initialConfig))
    val info = kerberosInfo.get
    KerberosMiniClusterSupport.writeSecureConfigs(configDir, info)
    KerberosMiniClusterSupport.loginLivyUser(info)
    extraJvmArgs = Seq(KerberosMiniClusterSupport.krb5JvmArg(info.krb5Conf))

    var updated = initialConfig ++ KerberosMiniClusterSupport.clusterProperties(info) ++
      KerberosMiniClusterSupport.livyKerberosProperties(info) ++ Map(
      "krb5ConfPath" -> info.krb5Conf.getAbsolutePath,
      KerberosMiniClusterSupport.KERBEROS_ENABLED_KEY -> "true")

    ozoneInfo = OzoneMiniClusterSupport.start(tempDir, configDir, kerberosInfo, updated)
    updated ++= ozoneInfo.map(OzoneMiniClusterSupport.clusterProperties).getOrElse(Map(
      OzoneMiniClusterSupport.OZONE_ENABLED_KEY -> "false",
      OzoneMiniClusterSupport.OZONE_STARTED_KEY -> "false"))

    hiveInfo = HiveMiniClusterSupport.start(tempDir, configDir, kerberosInfo, updated)
    updated ++= hiveInfo.map(HiveMiniClusterSupport.clusterProperties).getOrElse(Map(
      HiveMiniClusterSupport.HIVE_ENABLED_KEY -> "false",
      HiveMiniClusterSupport.HIVE_STARTED_KEY -> "false"))

    hbaseInfo = HBaseMiniClusterSupport.start(tempDir, configDir, kerberosInfo, updated)
    updated ++= hbaseInfo.map(HBaseMiniClusterSupport.clusterProperties).getOrElse(Map(
      HBaseMiniClusterSupport.HBASE_ENABLED_KEY -> "false",
      HBaseMiniClusterSupport.HBASE_STARTED_KEY -> "false"))

    kafkaInfo = KafkaMiniClusterSupport.start(tempDir, configDir, kerberosInfo, updated)
    updated ++= kafkaInfo.map(KafkaMiniClusterSupport.clusterProperties).getOrElse(Map(
      KafkaMiniClusterSupport.KAFKA_ENABLED_KEY -> "false",
      KafkaMiniClusterSupport.KAFKA_STARTED_KEY -> "false"))

    _authScheme = "kerberos"
    _clusterUser = "livy"
    _principal = info.livyPrincipal
    _keytabPath = info.keytabFile.getAbsolutePath

    // Patch spark-defaults.conf so that spark-submit authenticates to YARN using the
    // mini-cluster keytab (no credential cache exists when keytab-login-only is used),
    // and so that driver/executor JVMs can resolve the embedded KDC via krb5.conf.
    // Note: when a proxy-user is present Livy's ContextLauncher strips the principal/keytab
    // keys from the merged conf to avoid the Spark 3.5+ restriction that forbids combining
    // --proxy-user with --principal (SparkSubmitArguments line 285-286).
    val sparkDefaultsFile = new File(new File(tempDir, "spark-conf"), "spark-defaults.conf")
    if (sparkDefaultsFile.isFile) {
      val existing = loadProperties(sparkDefaultsFile)
      val krb5Opt = s"-Djava.security.krb5.conf=${info.krb5Conf.getAbsolutePath}"
      saveProperties(existing ++ Map(
        "spark.kerberos.principal" -> info.livyPrincipal,
        "spark.kerberos.keytab" -> info.keytabFile.getAbsolutePath,
        "spark.driver.extraJavaOptions" ->
          ((existing.getOrElse("spark.driver.extraJavaOptions", "") + " " + krb5Opt).trim),
        "spark.executor.extraJavaOptions" ->
          ((existing.getOrElse("spark.executor.extraJavaOptions", "") + " " + krb5Opt).trim)),
        sparkDefaultsFile)
    }

    updated
  }

  override protected def afterCleanup(): Unit = {
    KafkaMiniClusterSupport.stop(kafkaInfo)
    kafkaInfo = None
    HBaseMiniClusterSupport.stop(hbaseInfo)
    hbaseInfo = None
    HiveMiniClusterSupport.stop()
    hiveInfo = None
    OzoneMiniClusterSupport.stop(ozoneInfo)
    ozoneInfo = None
    KerberosMiniClusterSupport.stopKerberos(kerberosInfo)
    kerberosInfo = None
  }
}
