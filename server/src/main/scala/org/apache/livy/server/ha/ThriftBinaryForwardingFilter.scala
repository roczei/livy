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

package org.apache.livy.server.ha

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.net.Socket
import java.nio.ByteBuffer

import org.apache.livy.LivyConf
import org.apache.livy.Logging
import org.apache.livy.server.recovery.SessionStore

class ThriftBinaryForwardingFilter(
    allocator: Allocator,
    self: () => Option[ServerMetadata],
    sessionStore: SessionStore,
    livyConf: LivyConf)
  extends Logging {

  def shouldForward(buffer: Array[Byte]): Option[Int] = {
    extractSessionIdFromBinaryThrift(buffer)
  }

  def forwardToBinaryServer(buffer: Array[Byte],
                            owner: ServerMetadata,
                            sessionId: Int): Option[Array[Byte]] = {
    owner.thriftBinaryAddress match {
      case Some(address) =>
        forwardBinaryRequest(buffer, address, sessionId)
      case None =>
        warn(s"Owner ${owner.address} has no Thrift binary server configured")
        None
    }
  }

  private def extractSessionIdFromBinaryThrift(buffer: Array[Byte]): Option[Int] = {
    if (buffer.length < 12) {
      return None
    }

    try {
      val bb = ByteBuffer.wrap(buffer)

      val size = bb.getInt()
      if (size > buffer.length - 4 || size < 0) {
        return None
      }

      val version = bb.getInt()
      if ((version & 0xFFFF0000) != 0x80010000) {
        return None
      }

      val nameLength = bb.getInt()
      if (nameLength < 0 || nameLength > 1024) {
        return None
      }

      val nameBytes = new Array[Byte](nameLength)
      bb.get(nameBytes)
      val methodName = new String(nameBytes)

      if (methodName.contains("SessionHandle") || methodName.contains("OperationHandle")) {
        val sessionIdMatch = """sessionId[\":]?\s*(\d+)""".r.findFirstMatchIn(methodName)
        return sessionIdMatch.map(_.group(1).toInt)
      }

      bb.getInt()

      extractSessionIdFromThriftStruct(bb)
    } catch {
      case e: Exception =>
        debug(s"Failed to extract session ID from binary Thrift: ${e.getMessage}")
        None
    }
  }

  private def extractSessionIdFromThriftStruct(bb: ByteBuffer): Option[Int] = {
    try {
      while (bb.hasRemaining && bb.remaining() >= 6) {
        val fieldType = bb.get()
        if (fieldType == 0) {
          return None
        }

        val fieldId = bb.getShort()

        fieldType match {
          case 2 | 3 | 4 | 5 | 6 | 8 | 9 | 10 =>
            val value = bb.getInt()
            if (fieldId == 1 && fieldType == 8) {
              return Some(value)
            }
          case 11 | 12 =>
            val length = bb.getInt()
            if (length > 0 && length < 10000) {
              val bytes = new Array[Byte](length)
              bb.get(bytes)
              val str = new String(bytes)
              val match_ = """sessionId[\":]?\s*(\d+)""".r.findFirstMatchIn(str)
              if (match_.isDefined) {
                return match_.map(_.group(1).toInt)
              }
            }
          case _ =>
            return None
        }
      }
      None
    } catch {
      case _: Exception => None
    }
  }

  private def forwardBinaryRequest(buffer: Array[Byte],
                                   serverAddress: String,
                                   sessionId: Int): Option[Array[Byte]] = {
    var socket: Socket = null
    try {
      val parts = serverAddress.split(":")
      val host = parts(0)
      val port = parts(1).toInt

      socket = new Socket(host, port)
      val out = new BufferedOutputStream(socket.getOutputStream)
      val in = new BufferedInputStream(socket.getInputStream)

      out.write(buffer)
      out.flush()

      val responseBuffer = new Array[Byte](65536)
      val bytesRead = in.read(responseBuffer)

      if (bytesRead > 0) {
        Some(responseBuffer.take(bytesRead))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        error(s"Error forwarding binary Thrift request to $serverAddress: ${e.getMessage}", e)
        None
    } finally {
      if (socket != null) {
        try {
          socket.close()
        } catch {
          case _: Exception =>
        }
      }
    }
  }

  def isOwner(sessionId: Int): Boolean = {
    self() match {
      case Some(me) =>
        allocator.findServer(sessionId) match {
          case Some(owner) => me.key == owner.key
          case None => false
        }
      case None => false
    }
  }

  def getOwner(sessionId: Int): Option[ServerMetadata] = {
    allocator.findServer(sessionId)
  }
}
