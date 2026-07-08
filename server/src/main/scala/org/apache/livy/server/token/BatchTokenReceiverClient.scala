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

import java.io.{DataInputStream, DataOutputStream, IOException}
import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets
import java.util.Base64

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.livy.{LivyConf, Logging}

/**
 * Livy-side client for the [[org.apache.livy.tokenreceiver.LivyBatchTokenReceiver]]
 * running inside each batch Spark driver.
 *
 * <p>Reads the driver's mailbox file
 * (<code>&lt;mailbox-dir&gt;/session-&lt;appTag&gt;.info</code>) to discover the
 * driver's host, port and shared secret, then speaks the same framed HMAC
 * protocol as the receiver to push a refreshed Credentials blob into the driver
 * JVM. Mailbox reads are best-effort — if the mailbox is missing (driver hasn't
 * come up yet, or already terminated), the push is skipped for this cycle.
 *
 * <p>Mailbox locations are cached per session tag; a missing mailbox invalidates
 * the cache for that tag so the next cycle re-reads.
 */
class BatchTokenReceiverClient(livyConf: LivyConf) extends Logging {

  private val mapper = new ObjectMapper()
  private val mailboxCache = new TrieMap[String, MailboxEntry]()
  private val timeoutMs = livyConf.getTimeAsMs(LivyConf.TOKEN_RENEWAL_MAILBOX_TIMEOUT)

  private val Magic = "LIVYTOKN".getBytes(StandardCharsets.US_ASCII)
  private val RespOk: Byte = 0x00
  private val RespFail: Byte = 0xFF.toByte

  case class MailboxEntry(host: String, port: Int, secret: Array[Byte])

  /**
   * Push the given Credentials bytes to the batch driver identified by appTag.
   * Throws IOException if the push fails (mailbox missing, HMAC rejected, connection
   * refused, etc.). Callers should catch and log; failure to push in a single cycle
   * is not fatal — the next cycle will try again.
   */
  @throws[IOException]
  def push(appTag: String, credentialsBytes: Array[Byte]): Unit = {
    val entry = resolveMailbox(appTag).getOrElse {
      throw new IOException(s"No mailbox found for batch session tag $appTag " +
        s"(mailbox-dir=${mailboxDir()}).")
    }
    try {
      sendOne(entry, credentialsBytes)
    } catch {
      case e: IOException =>
        // The driver may have restarted with a new port/secret — invalidate the cache
        // and let the next cycle re-discover.
        mailboxCache.remove(appTag)
        throw e
    }
  }

  def close(): Unit = {
    mailboxCache.clear()
  }

  private def resolveMailbox(appTag: String): Option[MailboxEntry] = {
    mailboxCache.get(appTag).orElse {
      val mailbox = mailboxPath(appTag)
      val fs = mailbox.getFileSystem(livyConf.hadoopConf)
      if (!fs.exists(mailbox)) {
        debug(s"Batch token-receiver mailbox not found: $mailbox " +
          s"(driver may still be starting).")
        None
      } else {
        try {
          val entry = readMailbox(fs, mailbox)
          mailboxCache.put(appTag, entry)
          Some(entry)
        } catch {
          case NonFatal(e) =>
            warn(s"Failed to read batch token-receiver mailbox $mailbox", e)
            None
        }
      }
    }
  }

  private def readMailbox(fs: FileSystem, path: Path): MailboxEntry = {
    val in = fs.open(path)
    val bytes = try {
      val buf = new Array[Byte](in.available().max(256))
      val n = in.read(buf)
      if (n <= 0) throw new IOException(s"Empty mailbox file $path")
      java.util.Arrays.copyOf(buf, n)
    } finally in.close()

    val json = mapper.readTree(bytes)
    val host = json.get("host").asText()
    val port = json.get("port").asInt()
    val secretB64 = json.get("secret").asText()
    val secret = Base64.getDecoder.decode(secretB64)
    MailboxEntry(host, port, secret)
  }

  private def sendOne(entry: MailboxEntry, payload: Array[Byte]): Unit = {
    val sock = new Socket()
    try {
      sock.connect(new InetSocketAddress(entry.host, entry.port), timeoutMs.toInt)
      sock.setSoTimeout(timeoutMs.toInt)
      val out = new DataOutputStream(sock.getOutputStream)
      val in = new DataInputStream(sock.getInputStream)

      out.write(Magic)
      out.writeInt(payload.length)
      out.write(payload)

      val mac = Mac.getInstance("HmacSHA256")
      mac.init(new SecretKeySpec(entry.secret, "HmacSHA256"))
      mac.update(Magic)
      mac.update(intToBytes(payload.length))
      mac.update(payload)
      out.write(mac.doFinal())
      out.flush()

      val resp = in.readByte()
      if (resp != RespOk) {
        throw new IOException(s"Batch driver rejected token push: response=0x" +
          f"${resp & 0xff}%02x from ${entry.host}:${entry.port}")
      }
    } finally {
      try sock.close() catch { case _: IOException => }
    }
  }

  private def intToBytes(v: Int): Array[Byte] =
    Array[Byte](
      ((v >>> 24) & 0xff).toByte,
      ((v >>> 16) & 0xff).toByte,
      ((v >>> 8) & 0xff).toByte,
      (v & 0xff).toByte)

  private def mailboxDir(): String = {
    Option(livyConf.get(LivyConf.TOKEN_RENEWAL_MAILBOX_DIR)).getOrElse {
      val staging = Option(livyConf.get(LivyConf.SESSION_STAGING_DIR)).getOrElse {
        // Fall back to the Hadoop default: user's home directory on the default FS.
        val home = FileSystem.get(livyConf.hadoopConf).getHomeDirectory
        home.toString
      }
      new Path(staging, "livy-token-receivers").toString
    }
  }

  private def mailboxPath(appTag: String): Path = {
    new Path(mailboxDir(), s"session-$appTag.info")
  }
}
