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

package org.apache.livy.server.batch

import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Random

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

import org.apache.livy.{LivyConf, Logging, Utils}
import org.apache.livy.server.AccessManager
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.{FinishedSessionState, Session, SessionState}
import org.apache.livy.sessions.Session._
import org.apache.livy.utils.{AppInfo, SparkApp, SparkAppListener, SparkProcessBuilder}

@JsonIgnoreProperties(ignoreUnknown = true)
case class BatchRecoveryMetadata(
    id: Int,
    name: Option[String],
    appId: Option[String],
    appTag: String,
    owner: String,
    proxyUser: Option[String],
    version: Int = 1)
  extends RecoveryMetadata

object BatchSession extends Logging {
  val RECOVERY_SESSION_TYPE = "batch"
  private val TOKEN_RECEIVER_LISTENER_CLASS =
    "org.apache.livy.tokenreceiver.LivyBatchTokenReceiver"
  // batch session child processes number
  private val bscpn = new AtomicInteger

  def childProcesses(): AtomicInteger = {
    bscpn
  }

  /**
   * When session-level token renewal is enabled, injects the
   * [[org.apache.livy.tokenreceiver.LivyBatchTokenReceiver]] into the batch driver so
   * the Livy server can push refreshed delegation tokens to it via RPC. This is done
   * purely through Spark config (no --principal/--keytab, so the Livy service keytab
   * is never exposed to the proxy user):
   *
   *   spark.jars                            += <livy-token-receiver-jar>
   *   spark.extraListeners                  += LivyBatchTokenReceiver
   *   spark.livy.token-receiver.mailbox-dir = <hdfs mailbox parent>
   *   spark.livy.token-receiver.session-tag = <appTag>
   *
   * Works identically in client and cluster deploy modes: the listener runs inside
   * the driver JVM in both cases, and publishes its (host, port, secret) to an HDFS
   * mailbox that Livy reads regardless of the driver host.
   */
  private[livy] def injectTokenReceiver(
      conf: Map[String, String],
      appTag: String,
      livyConf: LivyConf): Map[String, String] = {
    if (!livyConf.getBoolean(LivyConf.TOKEN_RENEWAL_ENABLED)) {
      return conf
    }
    val listenerJar = Option(livyConf.get(LivyConf.TOKEN_RENEWAL_BATCH_LISTENER_JAR))
      .orElse(autoDetectListenerJar())
    listenerJar match {
      case None =>
        warn("Token renewal enabled but the livy-token-receiver JAR could not be " +
          "located (set livy.server.token-renewal.batch-listener.jar). Batch driver " +
          "will not receive refreshed tokens.")
        conf
      case Some(jar) =>
        val existingJars = conf.getOrElse("spark.jars", "")
        val mergedJars = Seq(existingJars, jar).filter(_.nonEmpty).mkString(",")
        val existingListeners = conf.getOrElse("spark.extraListeners", "")
        val mergedListeners = Seq(existingListeners, TOKEN_RECEIVER_LISTENER_CLASS)
          .filter(_.nonEmpty).mkString(",")
        val mailboxDir = Option(livyConf.get(LivyConf.TOKEN_RENEWAL_MAILBOX_DIR))
          .getOrElse(defaultMailboxDir(livyConf))
        val updated = conf ++ Map(
          "spark.jars" -> mergedJars,
          "spark.extraListeners" -> mergedListeners,
          "spark.livy.token-receiver.mailbox-dir" -> mailboxDir,
          "spark.livy.token-receiver.session-tag" -> appTag)
        info(s"Delegation token receiver injected for batch appTag=$appTag " +
          s"(listener JAR=$jar, mailbox=$mailboxDir).")
        updated
    }
  }

  private def autoDetectListenerJar(): Option[String] = {
    val livyHome = Option(System.getenv("LIVY_HOME")).orElse(sys.props.get("livy.home"))
    livyHome.flatMap { home =>
      val jarsDir = new java.io.File(home, "jars")
      if (!jarsDir.isDirectory) None
      else Option(jarsDir.listFiles()).flatMap { arr =>
        arr.find(f => f.getName.startsWith("livy-token-receiver") && f.getName.endsWith(".jar"))
          .map(_.getAbsolutePath)
      }
    }
  }

  private def defaultMailboxDir(livyConf: LivyConf): String = {
    Option(livyConf.get(LivyConf.SESSION_STAGING_DIR)) match {
      case Some(staging) => new org.apache.hadoop.fs.Path(staging, "livy-token-receivers").toString
      case None =>
        val home = org.apache.hadoop.fs.FileSystem.get(livyConf.hadoopConf).getHomeDirectory
        new org.apache.hadoop.fs.Path(home, "livy-token-receivers").toString
    }
  }

  def create(
      id: Int,
      name: Option[String],
      request: CreateBatchRequest,
      livyConf: LivyConf,
      accessManager: AccessManager,
      owner: String,
      proxyUser: Option[String],
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None): BatchSession = {
    val appTag = s"livy-batch-$id-${Random.alphanumeric.take(8).mkString}".toLowerCase()
    val impersonatedUser = accessManager.checkImpersonation(proxyUser, owner)

    def createSparkApp(s: BatchSession): SparkApp = {
      val baseConf = SparkApp.prepareSparkConf(
        appTag,
        livyConf,
        prepareConf(
          request.conf, request.jars, request.files, request.archives, request.pyFiles, livyConf))
      require(request.file != null, "File is required.")

      val conf = injectTokenReceiver(baseConf, appTag, livyConf)

      val builder = new SparkProcessBuilder(livyConf)
      builder.conf(conf)

      impersonatedUser.foreach(builder.proxyUser)
      request.className.foreach(builder.className)
      request.driverMemory.foreach(builder.driverMemory)
      request.driverCores.foreach(builder.driverCores)
      request.executorMemory.foreach(builder.executorMemory)
      request.executorCores.foreach(builder.executorCores)
      request.numExecutors.foreach(builder.numExecutors)
      request.queue.foreach(builder.queue)
      request.name.foreach(builder.name)

      sessionStore.save(BatchSession.RECOVERY_SESSION_TYPE, s.recoveryMetadata)

      builder.redirectOutput(Redirect.PIPE)
      builder.redirectErrorStream(true)

      val file = resolveURIs(Seq(request.file), livyConf)(0)
      val sparkSubmit = builder.start(Some(file), request.args)

      Utils.startDaemonThread(s"batch-session-process-$id") {
        childProcesses.incrementAndGet()
        try {
          sparkSubmit.waitFor() match {
            case 0 =>
            case exitCode =>
              warn(s"spark-submit exited with code $exitCode")
          }
        } finally {
          childProcesses.decrementAndGet()
        }
      }
      SparkApp.create(appTag, None, Option(sparkSubmit), livyConf, Option(s))
    }

    info(s"Creating batch session $id: [owner: $owner, request: $request]")

    new BatchSession(
      id,
      name,
      appTag,
      SessionState.Starting,
      livyConf,
      owner,
      impersonatedUser,
      sessionStore,
      mockApp.map { m => (_: BatchSession) => m }.getOrElse(createSparkApp))
  }

  def recover(
      m: BatchRecoveryMetadata,
      livyConf: LivyConf,
      sessionStore: SessionStore,
      mockApp: Option[SparkApp] = None): BatchSession = {
    new BatchSession(
      m.id,
      m.name,
      m.appTag,
      SessionState.Recovering,
      livyConf,
      m.owner,
      m.proxyUser,
      sessionStore,
      mockApp.map { m => (_: BatchSession) => m }.getOrElse { s =>
        SparkApp.create(m.appTag, m.appId, None, livyConf, Option(s))
      })
  }
}

class BatchSession(
    id: Int,
    name: Option[String],
    val appTag: String,
    initialState: SessionState,
    livyConf: LivyConf,
    owner: String,
    override val proxyUser: Option[String],
    sessionStore: SessionStore,
    sparkApp: BatchSession => SparkApp)
  extends Session(id, name, owner, livyConf) with SparkAppListener {
  import BatchSession._

  protected implicit def executor: ExecutionContextExecutor = ExecutionContext.global

  private[this] var _state: SessionState = initialState

  private var app: Option[SparkApp] = None

  override def state: SessionState = _state

  override def logLines(): IndexedSeq[String] = app.map(_.log()).getOrElse(IndexedSeq.empty[String])

  override def start(): Unit = {
    app = Option(sparkApp(this))
  }

  override def stopSession(): Unit = {
    app.foreach(_.kill())
  }

  override def appIdKnown(appId: String): Unit = {
    _appId = Option(appId)
    sessionStore.save(RECOVERY_SESSION_TYPE, recoveryMetadata)
  }

  override def stateChanged(oldState: SparkApp.State, newState: SparkApp.State): Unit = {
    synchronized {
      debug(s"$this state changed from $oldState to $newState")
      newState match {
        case SparkApp.State.RUNNING =>
          _state = SessionState.Running
          info(s"Batch session $id created [appid: ${appId.orNull}, state: ${state.toString}, " +
            s"info: ${appInfo.asJavaMap}]")
        case SparkApp.State.FINISHED => _state = SessionState.Success()
        case SparkApp.State.KILLED => {
          _state = SessionState.Killed()
          sessionStore.remove(RECOVERY_SESSION_TYPE, id)
        }
        case SparkApp.State.FAILED => {
          _state = SessionState.Dead()
          sessionStore.remove(RECOVERY_SESSION_TYPE, id)
        }
        case _ =>
      }
    }
  }

  override def infoChanged(appInfo: AppInfo): Unit = { this.appInfo = appInfo }

  override def recoveryMetadata: RecoveryMetadata =
    BatchRecoveryMetadata(id, name, appId, appTag, owner, proxyUser)
}
