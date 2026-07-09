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
import java.util.Properties

import scala.util.control.NonFatal

import org.apache.curator.test.TestingServer
import org.apache.hadoop.conf.Configuration

import org.apache.livy.Logging

/**
 * Embeds a Kafka broker into the Kerberos mini cluster (Maven test scope).
 */
private[framework] object KafkaMiniClusterSupport extends MiniClusterUtils with Logging {

  val KAFKA_ENABLED_KEY = "kafka.enabled"
  val KAFKA_STARTED_KEY = "kafka.started"

  private val TokenMaxLifetimeMs = TestDelegationTokenDefaults.MaxLifetimeMs.toString
  private val TokenExpiryTimeMs = TestDelegationTokenDefaults.ExpiryTimeMs.toString

  case class KafkaClusterInfo(
      bootstrapServers: String,
      zookeeper: TestingServer,
      broker: AnyRef,
      classpathJars: Seq[String])

  def isKafkaRequested(config: Map[String, String]): Boolean = {
    EmbeddedServiceSupport.isServiceRequested(config, KAFKA_ENABLED_KEY)
  }

  def isKafkaServerAvailable: Boolean = {
    EmbeddedServiceSupport.isClassAvailable("kafka.server.KafkaServer")
  }

  def start(
      workDir: File,
      configDir: File,
      kerberosInfo: Option[KerberosMiniClusterSupport.KerberosClusterInfo],
      config: Map[String, String]): Option[KafkaClusterInfo] = {
    if (!isKafkaRequested(config)) {
      return None
    }
    if (!isKafkaServerAvailable) {
      warn("Kafka is enabled but KafkaServer is not on the test classpath.")
      return None
    }

    var zk: TestingServer = null
    var broker: AnyRef = null
    try {
      val kafkaWorkDir = new File(workDir, "kafka")
      if (!kafkaWorkDir.mkdirs() && !kafkaWorkDir.isDirectory) {
        throw new IllegalStateException(s"Cannot create Kafka work dir $kafkaWorkDir")
      }

      zk = new TestingServer()
      val brokerProps = new Properties()
      brokerProps.setProperty("zookeeper.connect", zk.getConnectString)
      brokerProps.setProperty("broker.id", "0")
      brokerProps.setProperty("listeners", "PLAINTEXT://127.0.0.1:0")
      brokerProps.setProperty("log.dir", new File(kafkaWorkDir, "logs").getAbsolutePath)
      brokerProps.setProperty("offsets.topic.replication.factor", "1")
      brokerProps.setProperty("transaction.state.log.replication.factor", "1")
      brokerProps.setProperty("transaction.state.log.min.isr", "1")
      brokerProps.setProperty("num.network.threads", "2")
      brokerProps.setProperty("num.io.threads", "2")
      brokerProps.setProperty("delegation.token.max.lifetime.ms", TokenMaxLifetimeMs)
      brokerProps.setProperty("delegation.token.expiry.time.ms", TokenExpiryTimeMs)
      brokerProps.setProperty("delegation.token.master.key", "livy-it-kafka-master-key")

      kerberosInfo.foreach(info => configureSecureKafka(brokerProps, info, kafkaWorkDir))

      broker = startBroker(brokerProps)
      val boundPort = broker.getClass.getMethod("boundPort", classOf[String])
        .invoke(broker, "PLAINTEXT")
        .asInstanceOf[Int]
      val bootstrapServers = s"127.0.0.1:$boundPort"

      val kafkaConf = new Configuration(false)
      kafkaConf.set("kafka.bootstrap.servers", bootstrapServers)
      kafkaConf.set("delegation.token.max.lifetime.ms", TokenMaxLifetimeMs)
      kafkaConf.set("delegation.token.expiry.time.ms", TokenExpiryTimeMs)
      EmbeddedServiceSupport.mergeIntoCoreSite(configDir,
        Seq(
          "kafka.bootstrap.servers",
          "delegation.token.max.lifetime.ms",
          "delegation.token.expiry.time.ms"),
        kafkaConf)

      info(s"Embedded Kafka broker ready at $bootstrapServers")
      Some(KafkaClusterInfo(
        bootstrapServers,
        zk,
        broker,
        EmbeddedServiceSupport.runtimeJars(Seq("kafka"))))
    } catch {
      case NonFatal(e) =>
        if (broker != null) {
          stopBroker(broker)
        }
        if (zk != null) {
          try {
            zk.close()
          } catch {
            case NonFatal(_) =>
          }
        }
        warn("Failed to start embedded Kafka broker; Kafka IT will be skipped.", e)
        None
    }
  }

  def stop(info: Option[KafkaClusterInfo]): Unit = {
    info.foreach { i =>
      try {
        stopBroker(i.broker)
      } catch {
        case NonFatal(e) =>
          warn("Failed to shut down Kafka broker", e)
      }
      try {
        i.zookeeper.close()
      } catch {
        case NonFatal(e) =>
          warn("Failed to shut down Kafka ZooKeeper", e)
      }
    }
  }

  def clusterProperties(info: KafkaClusterInfo): Map[String, String] = Map(
    KAFKA_ENABLED_KEY -> "true",
    KAFKA_STARTED_KEY -> "true",
    "kafka.bootstrap.servers" -> info.bootstrapServers,
    "delegation.token.max.lifetime.ms" -> TokenMaxLifetimeMs,
    "delegation.token.expiry.time.ms" -> TokenExpiryTimeMs)

  private def configureSecureKafka(
      props: Properties,
      info: KerberosMiniClusterSupport.KerberosClusterInfo,
      workDir: File): Unit = {
    val hostRealm = s"${info.krbInstance}@${info.realm}"
    val keytab = info.keytabFile.getAbsolutePath
    val jaasFile = new File(workDir, "kafka-broker-jaas.conf")
    val jaas =
      s"""KafkaServer {
         |  com.sun.security.auth.module.Krb5LoginModule required
         |  useKeyTab=true
         |  storeKey=true
         |  keyTab="$keytab"
         |  principal="kafka/$hostRealm";
         |};
         |""".stripMargin
    val out = new java.io.FileOutputStream(jaasFile)
    try {
      out.write(jaas.getBytes("UTF-8"))
    } finally {
      out.close()
    }

    System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath)
    props.setProperty("listeners", "SASL_PLAINTEXT://127.0.0.1:0")
    props.setProperty("security.inter.broker.protocol", "SASL_PLAINTEXT")
    props.setProperty("sasl.mechanism.inter.broker.protocol", "GSSAPI")
    props.setProperty("sasl.enabled.mechanisms", "GSSAPI")
    props.setProperty("sasl.kerberos.service.name", "kafka")
  }

  private def startBroker(props: Properties): AnyRef = {
    val configClass = Class.forName("kafka.server.KafkaConfig")
    val config = configClass.getConstructor(classOf[Properties])
      .newInstance(props)
      .asInstanceOf[AnyRef]
    val timeClass = Class.forName("org.apache.kafka.common.utils.Time")
    val time = timeClass.getField("SYSTEM").get(null).asInstanceOf[AnyRef]
    val optionClass = Class.forName("scala.Option")
    val emptyOption = optionClass.getMethod("empty").invoke(null).asInstanceOf[AnyRef]
    val serverClass = Class.forName("kafka.server.KafkaServer")
    val broker = serverClass.getConstructor(
      configClass,
      timeClass,
      optionClass,
      classOf[Boolean]).newInstance(config, time, emptyOption, java.lang.Boolean.FALSE)
      .asInstanceOf[AnyRef]
    serverClass.getMethod("startup").invoke(broker)
    broker
  }

  private def stopBroker(broker: AnyRef): Unit = {
    broker.getClass.getMethod("shutdown").invoke(broker)
    broker.getClass.getMethod("awaitShutdown").invoke(broker)
  }
}
