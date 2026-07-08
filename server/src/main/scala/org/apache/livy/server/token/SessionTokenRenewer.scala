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

package org.apache.livy.server.token

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import scala.util.control.NonFatal

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.server.batch.BatchSession
import org.apache.livy.server.interactive.InteractiveSession
import org.apache.livy.sessions.{Session, SessionManager}

/**
 * Livy server side service that keeps sessions supplied with fresh Hadoop delegation
 * tokens for their proxy users. Runs periodically on a single daemon thread while the
 * Livy server is up; obtains tokens with the Livy service keytab (never exposing it to
 * proxy users) and pushes them into each active session's Spark driver via RPC:
 *
 *  - Interactive sessions: through the existing RSC channel
 *    ({@link InteractiveSession#updateDelegationTokens}).
 *  - Batch sessions: through the [[BatchTokenReceiverClient]] that reads the
 *    driver-published HDFS mailbox and speaks the same wire protocol as
 *    [[org.apache.livy.tokenreceiver.LivyBatchTokenReceiver]].
 *
 * Both spark-submit deploy modes (client and cluster) are supported: the mailbox is on
 * HDFS, so the driver's physical host is irrelevant.
 */
class SessionTokenRenewer(
    livyConf: LivyConf,
    sessionManagers: Seq[SessionManager[_, _]])
  extends Logging {

  private var executor: ScheduledExecutorService = _
  private val intervalMs =
    livyConf.getTimeAsMs(LivyConf.TOKEN_RENEWAL_INTERVAL)
  private val fsUris: Seq[URI] = fsUrisFromConf()

  private val batchTokenClient = new BatchTokenReceiverClient(livyConf)

  def start(): Unit = {
    if (!livyConf.getBoolean(LivyConf.TOKEN_RENEWAL_ENABLED)) {
      info("Session token renewer is disabled " +
        s"(${LivyConf.TOKEN_RENEWAL_ENABLED.key} = false).")
      return
    }
    if (!UserGroupInformation.isSecurityEnabled) {
      info("Session token renewer not started: Hadoop security is disabled.")
      return
    }
    executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, "livy-session-token-renewer")
        t.setDaemon(true)
        t
      }
    })
    // Start with a small initial delay so sessions have time to boot up before the first push.
    val initialDelayMs = math.min(intervalMs, 60L * 1000L)
    executor.scheduleWithFixedDelay(new RenewerTask,
      initialDelayMs, intervalMs, TimeUnit.MILLISECONDS)
    info(s"Session token renewer started (interval=${intervalMs}ms, " +
      s"initialDelay=${initialDelayMs}ms, fsUris=${fsUris.mkString(",")}).")
  }

  def stop(): Unit = {
    if (executor != null) {
      executor.shutdownNow()
      executor = null
    }
    batchTokenClient.close()
  }

  private class RenewerTask extends Runnable {
    override def run(): Unit = {
      try {
        renewAll()
      } catch {
        case NonFatal(e) => warn("Session token renewer cycle failed.", e)
      }
    }
  }

  /**
   * Perform one renewal cycle across all active sessions in all managers.
   * Failures for a single session are logged but do not abort the cycle.
   */
  private[token] def renewAll(): Unit = {
    for (mgr <- sessionManagers) {
      // `mgr.all()` is Iterable[_ <: Session]; go through Iterable[Session] explicitly to
      // help the Scala type-inferencer see .state on each element.
      val sessions: Iterable[Session] = mgr.all().asInstanceOf[Iterable[Session]]
      for (s <- sessions if s.state.isActive) {
        try {
          renewOne(s)
        } catch {
          case NonFatal(e) =>
            warn(s"Failed to renew delegation tokens for session ${s.id}", e)
        }
      }
    }
  }

  private def renewOne(session: Session): Unit = {
    val user = effectiveUser(session)
    val livyUgi = UserGroupInformation.getCurrentUser
    val credentials = obtainCredentialsForUser(user, livyUgi)
    if (credentials.numberOfTokens() == 0) {
      debug(s"No delegation tokens obtained for session ${session.id} (user=$user); skipping.")
      return
    }
    val bytes = credentialsToBytes(credentials)
    session match {
      case is: InteractiveSession =>
        is.updateDelegationTokens(bytes)
      case bs: BatchSession =>
        pushToBatch(bs, bytes)
      case _ =>
        debug(s"Session ${session.id} type not supported by token renewer; skipping.")
    }
  }

  private def pushToBatch(session: BatchSession, credentialsBytes: Array[Byte]): Unit = {
    try {
      batchTokenClient.push(session.appTag, credentialsBytes)
      info(s"Pushed refreshed delegation tokens to batch session ${session.id} " +
        s"(${credentialsBytes.length} bytes).")
    } catch {
      case NonFatal(e) =>
        warn(s"Failed to push delegation tokens to batch session ${session.id} " +
          s"(appTag=${session.appTag})", e)
    }
  }

  private def effectiveUser(session: Session): String = {
    session.proxyUser.getOrElse(session.owner)
  }

  /**
   * Obtain a fresh Credentials for the given user, using the Livy service UGI as a
   * proxying superuser. The Livy service principal must be configured as a Hadoop
   * proxy user (hadoop.proxyuser.livy.hosts / groups) for this to succeed.
   */
  private def obtainCredentialsForUser(
      user: String,
      livyUgi: UserGroupInformation): Credentials = {
    val proxyUgi = UserGroupInformation.createProxyUser(user, livyUgi)
    proxyUgi.doAs(new PrivilegedExceptionAction[Credentials] {
      override def run(): Credentials = {
        val creds = new Credentials()
        val renewer = livyUgi.getShortUserName
        val hadoopConf = livyConf.hadoopConf
        val uris = if (fsUris.nonEmpty) fsUris else Seq(FileSystem.get(hadoopConf).getUri)
        for (uri <- uris) {
          try {
            val fs = FileSystem.get(uri, hadoopConf)
            fs.addDelegationTokens(renewer, creds)
          } catch {
            case NonFatal(e) =>
              warn(s"Failed to obtain delegation token from $uri for user $user", e)
          }
        }
        creds
      }
    })
  }

  private def credentialsToBytes(creds: Credentials): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    try {
      creds.writeTokenStorageToStream(dos)
      dos.flush()
    } finally {
      dos.close()
    }
    baos.toByteArray
  }

  private def fsUrisFromConf(): Seq[URI] = {
    Option(livyConf.get(LivyConf.TOKEN_RENEWAL_FS_URIS))
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).map(URI.create).toSeq)
      .getOrElse(Seq.empty)
  }
}
