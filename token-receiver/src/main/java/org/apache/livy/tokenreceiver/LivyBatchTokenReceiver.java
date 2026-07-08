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

package org.apache.livy.tokenreceiver;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark {@code SparkListener} shipped into each Livy-managed batch driver.
 *
 * <p>On {@code onApplicationStart} it:
 * <ol>
 *   <li>Opens a listening TCP socket on an ephemeral port of the driver host.</li>
 *   <li>Generates a random 32-byte shared secret used to HMAC-authenticate
 *       incoming push requests from the Livy server.</li>
 *   <li>Writes a small JSON mailbox file to HDFS
 *       ({@code &lt;mailbox-dir&gt;/session-&lt;appTag&gt;.info}) containing the
 *       driver {@code host}, {@code port} and {@code secret}. The file is
 *       created with {@code 0600} permissions and owned by the driver's
 *       identity — in Livy's proxyuser model that means the file is owned by
 *       the proxy user (who cannot read files owned by the Livy service, but
 *       can obviously read this one). The Livy service, running as a Hadoop
 *       superuser, reads the file regardless of ownership.</li>
 *   <li>Starts an accept loop that services token-push requests forever.</li>
 * </ol>
 *
 * <p>Wire protocol (framed, all integers big-endian):
 *
 * <pre>
 *   client:
 *     [ 8-byte magic "LIVYTOKN" ]
 *     [ 4-byte int  length-of-payload ]
 *     [ N-byte      payload = Credentials.writeTokenStorageToStream() bytes ]
 *     [ 32-byte     HMAC_SHA256(secret, magic || length || payload) ]
 *   server:
 *     [ 1-byte      0x00=OK, 0xFF=fail ]
 * </pre>
 *
 * <p>The receiver validates the HMAC (constant-time compare), then installs
 * the credentials onto the driver JVM's current UGI and reflectively invokes
 * {@code SparkEnv.get().schedulerBackend().updateDelegationTokens(bytes)} so
 * Spark forwards the fresh tokens to every executor.
 *
 * <p>Configured via Spark conf entries set by
 * {@code BatchSession.createSparkApp}:
 * <ul>
 *   <li>{@code spark.livy.token-receiver.mailbox-dir} — HDFS parent dir.</li>
 *   <li>{@code spark.livy.token-receiver.session-tag} — Livy batch app tag,
 *       used as the mailbox file name.</li>
 *   <li>{@code spark.livy.token-receiver.port.min}
 *       / {@code spark.livy.token-receiver.port.max} — optional port range.
 *       Default: ephemeral (0).</li>
 * </ul>
 */
public class LivyBatchTokenReceiver extends SparkListener {

  private static final Logger LOG = LoggerFactory.getLogger(LivyBatchTokenReceiver.class);

  private static final String MAILBOX_DIR_KEY = "spark.livy.token-receiver.mailbox-dir";
  private static final String SESSION_TAG_KEY = "spark.livy.token-receiver.session-tag";
  private static final String PORT_MIN_KEY = "spark.livy.token-receiver.port.min";
  private static final String PORT_MAX_KEY = "spark.livy.token-receiver.port.max";

  private static final byte[] MAGIC = "LIVYTOKN".getBytes(StandardCharsets.US_ASCII);
  private static final byte RESP_OK = 0x00;
  private static final byte RESP_FAIL = (byte) 0xFF;
  private static final int MAX_PAYLOAD_BYTES = 1024 * 1024; // 1 MiB

  private final SparkConf sparkConf;
  private ServerSocket server;
  private byte[] secret;
  private volatile boolean stopped;
  private Thread acceptorThread;

  /**
   * Constructor required by Spark when the listener is added through
   * {@code spark.extraListeners}. Spark passes the driver's SparkConf.
   */
  public LivyBatchTokenReceiver(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart event) {
    String mailboxDir = sparkConf.get(MAILBOX_DIR_KEY, null);
    String sessionTag = sparkConf.get(SESSION_TAG_KEY, null);
    if (mailboxDir == null || sessionTag == null) {
      LOG.warn("LivyBatchTokenReceiver: mailbox-dir or session-tag not set; disabling. " +
          "Tokens will not be renewable for this session.");
      return;
    }
    try {
      startServer();
      writeMailbox(mailboxDir, sessionTag);
      startAcceptor();
      LOG.info("LivyBatchTokenReceiver listening on {}:{} for session tag {}",
          server.getInetAddress().getHostAddress(), server.getLocalPort(), sessionTag);
    } catch (Exception e) {
      LOG.error("LivyBatchTokenReceiver failed to start; tokens will not be renewable.", e);
      close();
    }
  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd event) {
    close();
  }

  private void startServer() throws IOException {
    int portMin = sparkConf.getInt(PORT_MIN_KEY, 0);
    int portMax = sparkConf.getInt(PORT_MAX_KEY, 0);
    IOException lastException = null;
    if (portMin == 0 && portMax == 0) {
      // Ephemeral — the mailbox will publish whatever port the OS chooses.
      server = new ServerSocket(0);
    } else {
      for (int p = Math.max(portMin, 1024); p <= portMax; p++) {
        try {
          server = new ServerSocket(p);
          break;
        } catch (IOException e) {
          lastException = e;
        }
      }
      if (server == null) {
        throw new IOException("No free port in range [" + portMin + ", " + portMax + "]",
            lastException);
      }
    }
    secret = new byte[32];
    new SecureRandom().nextBytes(secret);
  }

  private void writeMailbox(String mailboxDir, String sessionTag) throws IOException {
    Configuration hadoopConf = new Configuration();
    Path parent = new Path(mailboxDir);
    FileSystem fs = parent.getFileSystem(hadoopConf);
    if (!fs.exists(parent)) {
      fs.mkdirs(parent, new FsPermission("755"));
    }
    Path file = new Path(parent, "session-" + sessionTag + ".info");
    String host = InetAddress.getLocalHost().getHostAddress();
    int port = server.getLocalPort();
    String json = "{"
        + "\"host\":\"" + host + "\","
        + "\"port\":" + port + ","
        + "\"secret\":\"" + Base64.getEncoder().encodeToString(secret) + "\""
        + "}";
    // Atomic-ish publish: write to tmp then rename.
    Path tmp = new Path(parent, "session-" + sessionTag + ".info.tmp");
    try (FSDataOutputStream out = fs.create(tmp, true)) {
      out.write(json.getBytes(StandardCharsets.UTF_8));
    }
    fs.setPermission(tmp, new FsPermission("600"));
    if (fs.exists(file)) {
      fs.delete(file, false);
    }
    fs.rename(tmp, file);
    LOG.info("Published token-receiver mailbox {} as {}", file,
        UserGroupInformation.getCurrentUser().getShortUserName());
  }

  private void startAcceptor() {
    acceptorThread = new Thread(this::acceptLoop, "livy-token-receiver-acceptor");
    acceptorThread.setDaemon(true);
    acceptorThread.start();
  }

  private void acceptLoop() {
    while (!stopped) {
      try (Socket sock = server.accept()) {
        handleOne(sock);
      } catch (IOException e) {
        if (!stopped) {
          LOG.warn("LivyBatchTokenReceiver accept failed; continuing.", e);
        }
      } catch (Exception e) {
        LOG.error("LivyBatchTokenReceiver: unexpected error servicing token push.", e);
      }
    }
  }

  private void handleOne(Socket sock) throws Exception {
    sock.setSoTimeout(30000);
    try (DataInputStream in = new DataInputStream(sock.getInputStream());
         OutputStream out = sock.getOutputStream()) {
      byte[] magic = new byte[MAGIC.length];
      in.readFully(magic);
      if (!Arrays.equals(magic, MAGIC)) {
        out.write(RESP_FAIL);
        LOG.warn("Rejecting token push: bad magic from {}", sock.getRemoteSocketAddress());
        return;
      }
      int length = in.readInt();
      if (length <= 0 || length > MAX_PAYLOAD_BYTES) {
        out.write(RESP_FAIL);
        LOG.warn("Rejecting token push: bad payload length {}", length);
        return;
      }
      byte[] payload = new byte[length];
      in.readFully(payload);
      byte[] mac = new byte[32];
      in.readFully(mac);

      Mac hmac = Mac.getInstance("HmacSHA256");
      hmac.init(new SecretKeySpec(secret, "HmacSHA256"));
      hmac.update(magic);
      hmac.update(intToBytes(length));
      hmac.update(payload);
      byte[] expected = hmac.doFinal();
      if (!MessageDigest.isEqual(expected, mac)) {
        out.write(RESP_FAIL);
        LOG.warn("Rejecting token push: HMAC mismatch from {}", sock.getRemoteSocketAddress());
        return;
      }

      applyCredentials(payload);
      out.write(RESP_OK);
    }
  }

  private static byte[] intToBytes(int v) {
    return new byte[] {
        (byte) ((v >>> 24) & 0xFF),
        (byte) ((v >>> 16) & 0xFF),
        (byte) ((v >>> 8) & 0xFF),
        (byte) (v & 0xFF)
    };
  }

  /**
   * Installs the received Credentials on the current UGI and forwards the raw bytes
   * to Spark's scheduler backend so executors also pick up the new tokens. Uses
   * reflection to avoid a hard compile-time dependency on
   * CoarseGrainedSchedulerBackend.updateDelegationTokens whose visibility differs
   * across Spark versions.
   */
  private static void applyCredentials(byte[] credentialsBytes) throws Exception {
    Credentials creds = new Credentials();
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(credentialsBytes))) {
      creds.readTokenStorageStream(in);
    }
    UserGroupInformation.getCurrentUser().addCredentials(creds);
    LOG.info("Installed {} refreshed delegation tokens on the batch driver UGI.",
        creds.numberOfTokens());

    try {
      SparkEnv env = SparkEnv.get();
      if (env == null) {
        LOG.debug("SparkEnv not initialized; skipping executor token broadcast.");
        return;
      }
      Object backend = env.getClass().getMethod("schedulerBackend").invoke(env);
      if (backend == null) {
        LOG.debug("SchedulerBackend unavailable; skipping executor token broadcast.");
        return;
      }
      java.lang.reflect.Method update = null;
      Class<?> c = backend.getClass();
      while (c != null && c != Object.class) {
        try {
          update = c.getDeclaredMethod("updateDelegationTokens", byte[].class);
          break;
        } catch (NoSuchMethodException e) {
          c = c.getSuperclass();
        }
      }
      if (update == null) {
        LOG.debug("SchedulerBackend {} does not expose updateDelegationTokens; " +
            "skipping executor broadcast.", backend.getClass().getName());
        return;
      }
      update.setAccessible(true);
      update.invoke(backend, (Object) credentialsBytes);
      LOG.info("Broadcasted refreshed delegation tokens to Spark executors.");
    } catch (Exception e) {
      LOG.warn("Failed to broadcast refreshed tokens to executors " +
          "(driver-side UGI is still refreshed).", e);
    }
  }

  private synchronized void close() {
    stopped = true;
    try {
      if (server != null) {
        server.close();
      }
    } catch (IOException e) {
      LOG.debug("Error closing token-receiver server socket", e);
    }
    if (acceptorThread != null) {
      acceptorThread.interrupt();
    }
  }
}
