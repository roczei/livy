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

package org.apache.livy.test

import java.nio.file.Files
import java.nio.file.StandardCopyOption

import org.apache.livy.sessions.{SessionState, Spark}
import org.apache.livy.test.framework.TestDelegationTokenDefaults
import org.apache.livy.test.framework.{BaseIntegrationTestSuite, Cluster, LivyRestClient}

/**
 * Shared helpers for delegation token integration tests.
 */
trait DelegationTokenTestSupport { self: BaseIntegrationTestSuite =>

  // In the mini cluster the server already sets livy.spark.deploy-mode=cluster and
  // spark.submit.deployMode is blacklisted for clients, so we must not send it in requests.
  // For external clusters the caller controls deploy mode via Spark conf as usual.
  val DeployClient: Map[String, String] =
    if (Cluster.getConfig.getOrElse("cluster.type", "mini") == "mini") Map.empty
    else Map("spark.submit.deployMode" -> "client")
  val DeployCluster: Map[String, String] =
    if (Cluster.getConfig.getOrElse("cluster.type", "mini") == "mini") Map.empty
    else Map("spark.submit.deployMode" -> "cluster")

  /** Job runs longer than delegation token max lifetime (15s in Kerberos mini cluster). */
  val RenewalSleepSeconds: Int = 45
  val RenewalCheckIntervalSeconds: Int = 8

  def renewalSparkConf: Map[String, String] = Map(
    "spark.livy.test.renewal.sleep.seconds" -> RenewalSleepSeconds.toString,
    "spark.livy.test.renewal.check.interval.seconds" -> RenewalCheckIntervalSeconds.toString)

  def miniClusterMode: Boolean =
    Cluster.getConfig.getOrElse("cluster.type", "mini") == "mini"

  def testServices: Seq[String] = {
    val base = Seq("hdfs", "hive", "hbase", "kafka")
    if (miniClusterMode) {
      base.filter(serviceConfigured)
    } else {
      base
    }
  }

  def renewalServices: Seq[String] =
    testServices ++ (if (ozoneConfigured) Seq("ozone") else Nil)

  def hiveConfigured: Boolean =
    Cluster.getConfig.get("hive.started").contains("true") ||
      Cluster.getConfig.get("hive.metastore.uris").exists(_.nonEmpty)

  def hbaseConfigured: Boolean =
    Cluster.getConfig.get("hbase.started").contains("true") ||
      Cluster.getConfig.get("hbase.zookeeper.quorum").exists(_.nonEmpty)

  def kafkaConfigured: Boolean =
    Cluster.getConfig.get("kafka.started").contains("true") ||
      Cluster.getConfig.get("kafka.bootstrap.servers").exists(_.nonEmpty)

  private def serviceConfigured(service: String): Boolean = service.toLowerCase match {
    case "hdfs" => true
    case "hive" => hiveConfigured
    case "hbase" => hbaseConfigured
    case "kafka" => kafkaConfigured
    case "ozone" => ozoneConfigured
    case _ => false
  }

  def kerberosEnabled: Boolean = {
    Option(cluster).map(_.authScheme == "kerberos").getOrElse {
      Cluster.getConfig.getOrElse("authScheme", "") == "kerberos" ||
        (Cluster.getConfig.getOrElse("cluster.type", "mini") == "mini" &&
          Cluster.getConfig.get("kerberos.enabled").forall(_.toBoolean))
    }
  }

  def proxyUser: Option[String] = {
    val configured = Cluster.getConfig.get("livy.test.proxyUser").filter(_.nonEmpty)
      .orElse(sys.props.get("livy.test.proxyUser").filter(_.nonEmpty))
    if (kerberosEnabled) {
      configured.orElse(Some("proxy"))
    } else {
      None
    }
  }

  def impersonationProxy(impersonationMode: String): Option[String] = {
    if (impersonationMode == "impersonation") proxyUser else None
  }

  def serviceList: Seq[String] = {
    Cluster.getConfig.get("delegation.token.services")
      .orElse(sys.props.get("livy.test.delegationTokenServices"))
      .map(_.split(",").map(_.trim.toLowerCase).filter(_.nonEmpty).toSeq)
      .getOrElse(Seq("hdfs"))
  }

  def serviceEnabled(service: String): Boolean = serviceList.contains(service.toLowerCase)

  def ozoneConfigured: Boolean = {
    Cluster.getConfig.get("ozone.started").contains("true") &&
      Cluster.getConfig.get("livy.test.ozone.path").exists(_.nonEmpty)
  }

  def ozoneTestPath: String = {
    Cluster.getConfig.getOrElse("livy.test.ozone.path",
      throw new IllegalStateException("livy.test.ozone.path required for Ozone IT"))
  }

  def ozoneSparkConf: Map[String, String] = {
    if (!ozoneConfigured) {
      Map.empty
    } else {
      val path = ozoneTestPath
      val ozoneKeys = Seq(
        "ozone.om.service.ids",
        "ozone.om.address",
        "ozone.security.enabled",
        "ozone.scm.names",
        "ozone.scm.client.address")
      val fromCluster = Cluster.getConfig.collect {
        case (k, v) if ozoneKeys.contains(k) && v.nonEmpty => s"spark.hadoop.$k" -> v
      }
      Map("spark.livy.test.ozone.path" -> path) ++ fromCluster
    }
  }

  def serviceSparkConf: Map[String, String] = {
    val hdfsPath = s"${hdfsScratchPath}/dt-it-${System.currentTimeMillis()}"
    val base = Map(
      "spark.livy.test.services" -> serviceList.mkString(","),
      "spark.livy.test.hdfs.path" -> hdfsPath)
    val hive = Cluster.getConfig.get("hive.metastore.uris").filter(_.nonEmpty)
      .map("hive.metastore.uris" -> _).toMap
    val hbase = Cluster.getConfig.get("hbase.zookeeper.quorum").filter(_.nonEmpty)
      .map("hbase.zookeeper.quorum" -> _).toMap
    val kafka = Cluster.getConfig.get("kafka.bootstrap.servers").filter(_.nonEmpty)
      .map("kafka.bootstrap.servers" -> _).toMap
    val kafkaDelegationToken = {
      val keys = Seq(
        TestDelegationTokenDefaults.MaxLifetimeKey,
        TestDelegationTokenDefaults.ExpiryTimeKey)
      if (miniClusterMode) {
        keys.map { key =>
          val defaultMs = if (key == TestDelegationTokenDefaults.MaxLifetimeKey) {
            TestDelegationTokenDefaults.MaxLifetimeMs
          } else {
            TestDelegationTokenDefaults.ExpiryTimeMs
          }
          val value = Cluster.getConfig.getOrElse(key, defaultMs.toString)
          s"spark.hadoop.$key" -> value
        }.toMap
      } else {
        keys.flatMap { key =>
          Cluster.getConfig.get(key).map(v => s"spark.hadoop.$key" -> v)
        }.toMap
      }
    }
    base ++ hive ++ hbase ++ kafka ++ kafkaDelegationToken ++ ozoneSparkConf
  }

  def hdfsScratchPath: String = cluster.hdfsScratchDir().toString

  def uploadBatchScript(resourceName: String): String = {
    val tmp = Files.createTempFile("livy-dt-", ".py")
    val src = getClass.getClassLoader.getResourceAsStream(resourceName)
    require(src != null, s"Resource $resourceName not found")
    Files.copy(src, tmp, StandardCopyOption.REPLACE_EXISTING)
    uploadToHdfs(tmp.toFile)
  }

  def withInteractiveSession[R](
      deployMode: Map[String, String],
      proxy: Option[String],
      extraConf: Map[String, String] = Map.empty)
      (f: LivyRestClient#InteractiveSession => R): R = {
    val sparkConf = deployMode ++ serviceSparkConf ++ extraConf
    val session = livyClient.startSession(None, Spark, sparkConf, 0, proxy)
    withSession(session) { s =>
      s.verifySessionIdle()
      f(s)
    }
  }

  def withBatchSession[R](
      deployMode: Map[String, String],
      proxy: Option[String],
      script: String = "batch-dt-services.py",
      extraConf: Map[String, String] = Map.empty)
      (f: LivyRestClient#BatchSession => R): R = {
    val sparkConf = deployMode ++ serviceSparkConf ++ extraConf
    val file = uploadBatchScript(script)
    val session = livyClient.startBatch(None, file, None, Nil, sparkConf, proxy)
    withSession(session)(f)
  }

  def verifyNoFilesystemTokenPolling(s: LivyRestClient#InteractiveSession): Unit = {
    s.run("sc.hadoopConfiguration.get(\"livy.rsc.delegation.tokens.path\")")
      .verifyResult(".*null.*|.*res.*= null.*")
  }

  def verifyHdfsAccessInteractive(s: LivyRestClient#InteractiveSession): Unit = {
    val path = s"${hdfsScratchPath}/dt-it-${System.currentTimeMillis()}"
    s.run(s"""
      |val p = new org.apache.hadoop.fs.Path("$path")
      |val fs = p.getFileSystem(sc.hadoopConfiguration)
      |fs.mkdirs(p) && fs.exists(p)
      |""".stripMargin).verifyResult(".*true.*")
  }

  def verifyOzoneAccessInteractive(s: LivyRestClient#InteractiveSession): Unit = {
    assume(ozoneConfigured, "livy.test.ozone.path required in cluster.spec")
    val path = s"${ozoneTestPath}/dt-it-${System.currentTimeMillis()}"
    s.run(s"""
      |val p = new org.apache.hadoop.fs.Path("$path")
      |val fs = p.getFileSystem(sc.hadoopConfiguration)
      |fs.mkdirs(p) && fs.exists(p)
      |""".stripMargin).verifyResult(".*true.*")
  }

  def verifyHiveAccessInteractive(s: LivyRestClient#InteractiveSession): Unit = {
    assume(serviceEnabled("hive"), "hive not enabled in delegation.token.services")
    assume(hiveConfigured, "Embedded Hive Metastore not started")
    s.run("spark.sql(\"SHOW DATABASES\").count()").verifyResult(".*[1-9].*")
  }

  def verifyHbaseAccessInteractive(s: LivyRestClient#InteractiveSession): Unit = {
    assume(serviceEnabled("hbase"), "hbase not enabled in delegation.token.services")
    assume(hbaseConfigured, "Embedded HBase mini cluster not started")
    s.run("""
      |val conf = sc.hadoopConfiguration
      |val conn = org.apache.hadoop.hbase.client.ConnectionFactory
      |  .createConnection(org.apache.hadoop.hbase.HBaseConfiguration.create(conf))
      |try {
      |  val admin = conn.getAdmin
      |  admin.listNamespaceDescriptors().length >= 0
      |} finally {
      |  conn.close()
      |}
      |""".stripMargin).verifyResult(".*true.*")
  }

  def verifyKafkaAccessInteractive(s: LivyRestClient#InteractiveSession): Unit = {
    assume(serviceEnabled("kafka"), "kafka not enabled in delegation.token.services")
    assume(kafkaConfigured, "Embedded Kafka broker not started")
    val bootstrap = Cluster.getConfig.get("kafka.bootstrap.servers")
      .getOrElse(throw new IllegalStateException("kafka.bootstrap.servers required"))
    s.run(s"""
      |val props = new java.util.Properties()
      |props.put("bootstrap.servers", "$bootstrap")
      |val consumer = org.apache.kafka.clients.consumer.KafkaConsumer
      |  .class.getConstructor(classOf[java.util.Properties])
      |  .newInstance(props)
      |consumer.close()
      |"ok"
      |""".stripMargin).verifyResult(".*ok.*")
  }

  def verifyServiceAccessInteractive(
      s: LivyRestClient#InteractiveSession,
      service: String): Unit = {
    service.toLowerCase match {
      case "hdfs" => verifyHdfsAccessInteractive(s)
      case "ozone" => verifyOzoneAccessInteractive(s)
      case "hive" => verifyHiveAccessInteractive(s)
      case "hbase" => verifyHbaseAccessInteractive(s)
      case "kafka" => verifyKafkaAccessInteractive(s)
      case other => fail(s"Unknown service $other")
    }
  }

  def verifyBatchSuccess(
      deployMode: Map[String, String],
      proxy: Option[String],
      script: String = "batch-dt-services.py"): Unit = {
    withBatchSession(deployMode, proxy, script) { batch =>
      batch.verifySessionState(SessionState.Success())
    }
  }

  def verifyRenewalBatch(
      deployMode: Map[String, String],
      proxy: Option[String],
      service: String): Unit = {
    assume(proxy.isDefined, "Renewal IT requires impersonation (proxy user)")
    val narrowedConf = Map("spark.livy.test.services" -> service) ++ renewalSparkConf
    withBatchSession(
        deployMode,
        proxy,
        script = "batch-dt-renewal.py",
        extraConf = narrowedConf) { batch =>
      batch.verifySessionState(SessionState.Success())
    }
  }

  def verifyRenewalInteractive(
      s: LivyRestClient#InteractiveSession,
      service: String): Unit = {
    val sleepSec = RenewalSleepSeconds
    val intervalSec = RenewalCheckIntervalSeconds
    service.toLowerCase match {
      case "hdfs" =>
        val path = s"${hdfsScratchPath}/dt-renewal-${System.currentTimeMillis()}"
        s.run(s"""
          |import java.util.concurrent.TimeUnit
          |val deadline = System.currentTimeMillis() + ${sleepSec * 1000}L
          |var checks = 0
          |var ok = true
          |while (System.currentTimeMillis() < deadline && ok) {
          |  val p = new org.apache.hadoop.fs.Path("$path/" + checks)
          |  val fs = p.getFileSystem(sc.hadoopConfiguration)
          |  ok = fs.mkdirs(p) && fs.exists(p)
          |  checks += 1
          |  val remaining = deadline - System.currentTimeMillis()
          |  if (remaining > 0) {
          |    TimeUnit.MILLISECONDS.sleep(math.min(${intervalSec * 1000}L, remaining))
          |  }
          |}
          |checks >= 2 && ok
          |""".stripMargin).verifyResult(".*true.*")
      case "ozone" =>
        val base = ozoneTestPath
        s.run(s"""
          |import java.util.concurrent.TimeUnit
          |val deadline = System.currentTimeMillis() + ${sleepSec * 1000}L
          |var checks = 0
          |var ok = true
          |while (System.currentTimeMillis() < deadline && ok) {
          |  val scratch = "$base/renewal-" + checks
          |  val p = new org.apache.hadoop.fs.Path(scratch)
          |  val fs = p.getFileSystem(sc.hadoopConfiguration)
          |  ok = fs.mkdirs(p) && fs.exists(p)
          |  checks += 1
          |  val remaining = deadline - System.currentTimeMillis()
          |  if (remaining > 0) {
          |    TimeUnit.MILLISECONDS.sleep(math.min(${intervalSec * 1000}L, remaining))
          |  }
          |}
          |checks >= 2 && ok
          |""".stripMargin).verifyResult(".*true.*")
      case other =>
        assume(false, s"Interactive renewal not implemented for $other; use batch IT")
    }
  }

  def assumeKerberosForImpersonation(proxy: Option[String]): Unit = {
    if (proxy.isDefined) {
      assume(kerberosEnabled, "Impersonation tests require authScheme=kerberos in cluster.spec")
    }
  }

  def assumeServiceConfigured(service: String): Unit = {
    assume(serviceEnabled(service), s"$service not listed in delegation.token.services")
    service.toLowerCase match {
      case "hdfs" =>
      case "ozone" =>
        assume(ozoneConfigured,
          "Embedded MiniOzoneCluster not started")
      case "hive" =>
        assume(hiveConfigured, "Embedded Hive Metastore not started")
      case "hbase" =>
        assume(hbaseConfigured, "Embedded HBase mini cluster not started")
      case "kafka" =>
        assume(kafkaConfigured, "Embedded Kafka broker not started")
      case other =>
        fail(s"Unknown service $other")
    }
  }
}
