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

import java.security.MessageDigest

import scala.collection.immutable.TreeMap

import org.apache.livy.LivyConf

class ConsistentHashingAllocator(livyConf: LivyConf) extends Allocator {
  private val virtualNodes = livyConf.getInt(LivyConf.HA_ALLOCATOR_VIRTUAL_NODES)

  @volatile
  private var ring: TreeMap[Long, ServerMetadata] = TreeMap.empty

  private def hash(input: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(input.getBytes("UTF-8"))
    var hash = 0L
    for (i <- 0 until 8) {
      hash = (hash << 8) | (digest(i) & 0xFF)
    }
    hash
  }

  private def hashSessionId(sessionId: Int): Long = {
    hash(sessionId.toString)
  }

  private def findOwner(hashValue: Long): Option[ServerMetadata] = {
    val current = ring
    if (current.isEmpty) {
      None
    } else {
      current.from(hashValue).headOption.map(_._2).orElse(current.headOption.map(_._2))
    }
  }

  def findServer(sessionId: Int): Option[ServerMetadata] = {
    findOwner(hashSessionId(sessionId))
  }

  def allocateServer(sessionId: Int): ServerMetadata = {
    findServer(sessionId).getOrElse {
      throw new IllegalStateException("No servers available in the consistent hash ring")
    }
  }

  def onServerJoin(server: ServerMetadata): Unit = synchronized {
    val newRing = (0 until virtualNodes).foldLeft(ring) { (acc, i) =>
      val virtualKey = hash(s"${server.key}#$i")
      acc + (virtualKey -> server)
    }
    ring = newRing
  }

  def onServerLeave(server: ServerMetadata): Unit = synchronized {
    val newRing = (0 until virtualNodes).foldLeft(ring) { (acc, i) =>
      val virtualKey = hash(s"${server.key}#$i")
      acc - virtualKey
    }
    ring = newRing
  }
}
