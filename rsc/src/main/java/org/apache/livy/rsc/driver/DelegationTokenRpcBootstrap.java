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

package org.apache.livy.rsc.driver;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import scala.Tuple2;

import io.netty.channel.ChannelHandlerContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.livy.rsc.BaseProtocol;
import org.apache.livy.rsc.RSCConf;
import org.apache.livy.rsc.rpc.Rpc;
import org.apache.livy.rsc.rpc.RpcDispatcher;
import org.apache.livy.rsc.rpc.RpcServer;

import static org.apache.livy.rsc.RSCConf.Entry.*;

/**
 * Batch-mode driver bootstrap for delegation token refresh over encrypted RSC RPC.
 *
 * Initial delegation tokens are expected to be provisioned by Spark launch (--proxy-user).
 * Livy pushes renewed tokens via {@link BaseProtocol.UpdateCredentialsRequest}.
 */
public class DelegationTokenRpcBootstrap extends SparkListener {

  private static final Logger LOG = LoggerFactory.getLogger(DelegationTokenRpcBootstrap.class);
  private static final String LIVY_SPARK_PREFIX = "spark.__livy__.";

  private volatile RpcServer server;

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    try {
      SparkConf sparkConf = SparkEnv.get().conf();
      RSCConf livyConf = loadLivyConf(sparkConf);
      if (!isConfigured(livyConf)) {
        LOG.debug("Delegation token RPC bootstrap disabled; launcher settings are missing.");
        return;
      }
      initializeServer(livyConf, sparkConf);
    } catch (Exception e) {
      LOG.warn("Failed to start delegation token RPC bootstrap", e);
    }
  }

  static boolean isConfigured(RSCConf livyConf) {
    return livyConf.get(LAUNCHER_ADDRESS) != null
      && livyConf.getInt(LAUNCHER_PORT) > 0
      && livyConf.get(CLIENT_ID) != null
      && livyConf.get(CLIENT_SECRET) != null;
  }

  static RSCConf loadLivyConf(SparkConf sparkConf) {
    RSCConf livyConf = new RSCConf();
    for (scala.Tuple2<String, String> entry : sparkConf.getAll()) {
      if (entry._1().startsWith(LIVY_SPARK_PREFIX)) {
        livyConf.set(entry._1().substring(LIVY_SPARK_PREFIX.length()), entry._2());
      }
    }
    return livyConf;
  }

  private void initializeServer(RSCConf livyConf, SparkConf sparkConf) throws Exception {
    String clientId = livyConf.get(CLIENT_ID);
    String secret = livyConf.get(CLIENT_SECRET);
    String launcherAddress = livyConf.get(LAUNCHER_ADDRESS);
    int launcherPort = livyConf.getInt(LAUNCHER_PORT);

    livyConf.set(RPC_SERVER_ADDRESS, null);

    String master = sparkConf.get("spark.master", "");
    String deployMode = sparkConf.get("spark.submit.deployMode", "");
    if (master.startsWith("k8s") || "cluster".equalsIgnoreCase(deployMode)) {
      String driverHost = sparkConf.get("spark.driver.host");
      if (driverHost != null && !driverHost.trim().isEmpty()) {
        livyConf.set(RPC_SERVER_ADDRESS, driverHost);
      }
    }

    LOG.info("Starting delegation token RPC server for batch driver");
    this.server = new RpcServer(livyConf);
    final CredentialRpcHandler handler = new CredentialRpcHandler();
    server.registerClient(clientId, secret, new RpcServer.ClientCallback() {
      @Override
      public RpcDispatcher onNewClient(Rpc client) {
        return handler;
      }

      @Override
      public void onSaslComplete(Rpc client) {
      }
    });

    Rpc callbackRpc = Rpc.createClient(
      livyConf,
      server.getEventLoopGroup(),
      launcherAddress,
      launcherPort,
      clientId,
      secret,
      handler).get();
    try {
      callbackRpc.call(
        new BaseProtocol.RemoteDriverAddress(server.getAddress(), server.getPort())).get(
          livyConf.getTimeAsMs(RPC_CLIENT_HANDSHAKE_TIMEOUT), TimeUnit.MILLISECONDS);
      LOG.info("Registered batch driver for delegation token RPC at {}:{}",
        server.getAddress(), server.getPort());
    } catch (TimeoutException te) {
      LOG.warn("Timed out registering batch driver with Livy server.", te);
      throw te;
    } finally {
      callbackRpc.close();
    }
  }

  public static void applyCredentials(byte[] serializedCredentials) throws Exception {
    if (serializedCredentials == null || serializedCredentials.length == 0) {
      return;
    }
    Credentials creds = new Credentials();
    creds.readTokenStorageStream(
      new DataInputStream(new ByteArrayInputStream(serializedCredentials)));
    UserGroupInformation.getCurrentUser().addCredentials(creds);
    LOG.info("Applied delegation tokens from Livy RPC to driver UGI");
  }

  private static final class CredentialRpcHandler extends BaseProtocol {
    public void handle(ChannelHandlerContext ctx, UpdateCredentialsRequest msg) throws Exception {
      applyCredentials(msg.serializedCredentials);
    }
  }
}
