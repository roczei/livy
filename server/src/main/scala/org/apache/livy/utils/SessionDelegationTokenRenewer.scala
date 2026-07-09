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

import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.Executors

import scala.util.control.NonFatal

import org.apache.hadoop.security.Credentials

import org.apache.livy.{LivyConf, Logging}

/**
 * Per-session delegation token lifecycle manager. Obtains and renews tokens server-side, then
 * pushes updates to the Spark driver over encrypted RSC RPC.
 */
class SessionDelegationTokenRenewer(
    sessionId: Int,
    proxyUser: String,
    livyConf: LivyConf,
    batchMode: Boolean,
    sessionFilesystemUris: Set[String] = Set.empty) extends Logging {

  private var credentialPusher: Option[Array[Byte] => Unit] = None

  private val scheduler: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(r => {
      val t = new Thread(r, s"livy-delegation-token-renewer-$sessionId")
      t.setDaemon(true)
      t
    })

  @volatile private var credentials: Credentials = _
  @volatile private var renewalTask: ScheduledFuture[_] = _
  @volatile private var stopped = false

  def registerCredentialPusher(pusher: Array[Byte] => Unit): Unit = {
    credentialPusher = Some(pusher)
    Option(credentials).foreach(creds => pusher(DelegationTokenManager.serialize(creds)))
  }

  def start(existingSparkConf: Map[String, String] = Map.empty): Map[String, String] = {
    credentials = DelegationTokenManager.obtainTokens(proxyUser, livyConf, sessionFilesystemUris)
    scheduleRenewal()
    if (batchMode) {
      DelegationTokenRpcRegistry.sparkConfForBatch(sessionId, this, livyConf, existingSparkConf)
    } else {
      Map.empty
    }
  }

  def stop(): Unit = {
    stopped = true
    if (batchMode) {
      DelegationTokenRpcRegistry.unregisterSession(sessionId)
    }
    Option(renewalTask).foreach(_.cancel(false))
    scheduler.shutdownNow()
  }

  private def scheduleRenewal(): Unit = {
    val defaultInterval = livyConf.getTimeAsMs(LivyConf.DELEGATION_TOKEN_RENEWAL_INTERVAL)
    val interval = DelegationTokenManager.nextRenewalTime(credentials, livyConf)
      .map(math.min(_, defaultInterval))
      .getOrElse(defaultInterval)

    renewalTask = scheduler.schedule(new Runnable {
      override def run(): Unit = {
        if (stopped) return
        try {
          renewAndPush()
        } catch {
          case NonFatal(e) =>
            warn(s"Delegation token renewal failed for session $sessionId", e)
        } finally {
          if (!stopped) {
            scheduleRenewal()
          }
        }
      }
    }, interval, TimeUnit.MILLISECONDS)
  }

  private def renewAndPush(): Unit = {
    DelegationTokenManager.renewTokens(credentials, livyConf)
    DelegationTokenManager.refreshSessionFilesystemTokens(
      proxyUser, livyConf, sessionFilesystemUris, credentials)
    credentialPusher.foreach(_.apply(DelegationTokenManager.serialize(credentials)))
    info(s"Renewed delegation tokens for session $sessionId (proxyUser=$proxyUser)")
  }
}

object SessionDelegationTokenRenewer {
  def createIfNeeded(
      sessionId: Int,
      proxyUser: Option[String],
      livyConf: LivyConf,
      batchMode: Boolean,
      sessionFilesystemUris: Set[String] = Set.empty): Option[SessionDelegationTokenRenewer] = {
    if (DelegationTokenManager.isEnabled(livyConf, proxyUser)) {
      Some(new SessionDelegationTokenRenewer(
        sessionId, proxyUser.get, livyConf, batchMode, sessionFilesystemUris))
    } else {
      None
    }
  }

  def prepareForLaunch(
      sessionId: Int,
      proxyUser: Option[String],
      livyConf: LivyConf,
      batchMode: Boolean,
      existingSparkConf: Map[String, String] = Map.empty,
      sessionFilesystemUris: Set[String] = Set.empty)
  : (Map[String, String], Option[SessionDelegationTokenRenewer]) = {
    val renewer = createIfNeeded(sessionId, proxyUser, livyConf, batchMode, sessionFilesystemUris)
    val conf = renewer.map(_.start(existingSparkConf)).getOrElse(Map.empty)
    (conf, renewer)
  }
}
