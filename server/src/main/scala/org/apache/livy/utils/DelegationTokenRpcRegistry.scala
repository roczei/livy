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

import java.util.UUID
import java.util.concurrent.{TimeoutException, TimeUnit}

import scala.collection.mutable
import scala.util.control.NonFatal

import io.netty.channel.ChannelHandlerContext

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.rsc.{BaseProtocol, RSCConf}
import org.apache.livy.rsc.BaseProtocol.RemoteDriverAddress
import org.apache.livy.rsc.rpc.{Rpc, RpcDispatcher, RpcServer}

/**
 * Accepts batch driver registrations and maintains encrypted RPC channels for pushing renewed
 * delegation tokens from Livy to Spark drivers.
 */
object DelegationTokenRpcRegistry extends Logging {

  private case class PendingSession(
      sessionId: Int,
      secret: String,
      renewer: SessionDelegationTokenRenewer)

  @volatile private var rpcServer: RpcServer = _
  private val pendingSessions = mutable.Map[String, PendingSession]()
  private val driverRpcBySession = mutable.Map[Int, Rpc]()

  private def livySparkKey(entry: RSCConf.Entry): String = {
    s"${RSCConf.LIVY_SPARK_PREFIX}${RSCConf.RSC_CONF_PREFIX}${entry.key()}"
  }

  def sparkConfForBatch(
      sessionId: Int,
      renewer: SessionDelegationTokenRenewer,
      livyConf: LivyConf,
      existingSparkConf: Map[String, String] = Map.empty): Map[String, String] = {
    val server = ensureServer()
    val clientId = UUID.randomUUID().toString
    val secret = server.createSecret()
    pendingSessions(clientId) = PendingSession(sessionId, secret, renewer)
    server.registerClient(clientId, secret, new RegistrationHandler(clientId))

    val listener = existingSparkConf.get(DelegationTokenManager.SPARK_EXTRA_LISTENERS_KEY) match {
      case Some(existing: String) if existing.nonEmpty =>
        s"$existing,${DelegationTokenManager.DELEGATION_TOKEN_RPC_BOOTSTRAP_CLASS}"
      case _ => DelegationTokenManager.DELEGATION_TOKEN_RPC_BOOTSTRAP_CLASS
    }

    val updates = mutable.Map(
      livySparkKey(RSCConf.Entry.LAUNCHER_ADDRESS) -> server.getAddress,
      livySparkKey(RSCConf.Entry.LAUNCHER_PORT) -> server.getPort.toString,
      livySparkKey(RSCConf.Entry.CLIENT_ID) -> clientId,
      livySparkKey(RSCConf.Entry.CLIENT_SECRET) -> secret,
      DelegationTokenManager.SPARK_EXTRA_LISTENERS_KEY -> listener)

    DelegationTokenManager.rscJarPaths(livyConf).foreach { jars =>
      val merged = existingSparkConf.get(LivyConf.SPARK_JARS) match {
        case Some(existing: String) if existing.nonEmpty => s"$existing,$jars"
        case _ => jars
      }
      updates(LivyConf.SPARK_JARS) = merged
    }
    updates.toMap
  }

  def unregisterSession(sessionId: Int): Unit = synchronized {
    driverRpcBySession.remove(sessionId).foreach(closeRpc)
    pendingSessions.retain { case (_, pending) => pending.sessionId != sessionId }
  }

  private def ensureServer(): RpcServer = synchronized {
    if (rpcServer == null) {
      try {
        rpcServer = new RpcServer(new RSCConf())
        info(s"Delegation token RPC registry listening on ${rpcServer.getAddress}:" +
          s"${rpcServer.getPort}")
      } catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          throw e
      }
    }
    rpcServer
  }

  private def onDriverRegistered(
      clientId: String,
      driverHost: String,
      driverPort: Int): Unit = {
    val pending = pendingSessions.remove(clientId).getOrElse {
      warn(s"Ignoring delegation token RPC registration for unknown client $clientId")
      return
    }

    try {
      val rscConf = new RSCConf()
      val rpc = Rpc.createClient(
        rscConf,
        rpcServer.getEventLoopGroup,
        driverHost,
        driverPort,
        clientId,
        pending.secret,
        new RpcDispatcher() {}).get(
          rscConf.getTimeAsMs(RSCConf.Entry.RPC_CLIENT_CONNECT_TIMEOUT),
          TimeUnit.MILLISECONDS)

      driverRpcBySession.remove(pending.sessionId).foreach(closeRpc)
      driverRpcBySession(pending.sessionId) = rpc
      pending.renewer.registerCredentialPusher { bytes =>
        pushCredentials(rpc, bytes)
      }
      info(s"Batch session ${pending.sessionId} connected for delegation token RPC")
    } catch {
      case NonFatal(e) =>
        warn(s"Failed to connect to batch driver for session ${pending.sessionId}", e)
    }
  }

  private def pushCredentials(rpc: Rpc, bytes: Array[Byte]): Unit = {
    try {
      rpc.call(new BaseProtocol.UpdateCredentialsRequest(bytes), classOf[Void]).get()
    } catch {
      case NonFatal(e) =>
        warn("Failed to push delegation tokens to batch driver", e)
    }
  }

  private def closeRpc(rpc: Rpc): Unit = {
    try {
      rpc.close()
    } catch {
      case NonFatal(e) =>
        debug("Error closing delegation token RPC channel", e)
    }
  }

  private class RegistrationHandler(clientId: String)
    extends BaseProtocol with RpcServer.ClientCallback {

    override def onNewClient(client: Rpc): RpcDispatcher = this

    override def onSaslComplete(client: Rpc): Unit = {}

    def handle(ctx: ChannelHandlerContext, msg: RemoteDriverAddress): Unit = {
      val host = Option(msg.host).filter(_.nonEmpty).getOrElse {
        val remote = ctx.channel().remoteAddress().asInstanceOf[java.net.InetSocketAddress]
        remote.getAddress.getHostAddress
      }
      onDriverRegistered(clientId, host, msg.port)
      ctx.executor().submit(new Runnable {
        override def run(): Unit = ctx.channel().close()
      })
    }
  }
}
