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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{CuratorCache, CuratorCacheListener}
import org.apache.curator.framework.recipes.nodes.PersistentNode
import org.apache.zookeeper.CreateMode

import org.apache.livy.LivyConf
import org.apache.livy.Logging
import org.apache.livy.sessions.SessionKindModule

class ZkServerRegistrar(
    curator: CuratorFramework,
    livyConf: LivyConf,
    allocator: Allocator)
  extends Logging {

  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  private val serversPath = livyConf.get(LivyConf.HA_ZK_SERVERS_PATH)
  private var node: Option[PersistentNode] = None
  private var cache: Option[CuratorCache] = None
  private var knownServers: Set[ServerMetadata] = Set.empty
  private var self: Option[ServerMetadata] = None

  private def serializeToBytes(value: Object): Array[Byte] = mapper.writeValueAsBytes(value)
  private def deserialize[T: ClassTag](json: Array[Byte]): T = {
    mapper.readValue(json, scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]])
  }

  def register(metadata: ServerMetadata): Unit = synchronized {
    self = Some(metadata)
    val data = serializeToBytes(metadata)
    val persistentNode = new PersistentNode(
      curator,
      CreateMode.EPHEMERAL_SEQUENTIAL,
      true,
      s"$serversPath/server-",
      data)
    persistentNode.start()
    node = Some(persistentNode)
    info(s"Registered server: ${metadata.address} with uuid: ${metadata.uuid}")

    if (cache.isEmpty) {
      val curator_cache = CuratorCache.build(curator, serversPath)
      curator_cache.listenable().addListener(
        (eventType: CuratorCacheListener.Type, old: Any, new_ : Any) => {
          syncServers()
        })
      curator_cache.start()
      cache = Some(curator_cache)
      syncServers()
    }
  }

  private def syncServers(): Unit = synchronized {
    try {
      val children = curator.getChildren.forPath(serversPath).asScala.map { childPath =>
        val fullPath = s"$serversPath/$childPath"
        Option(curator.getData.forPath(fullPath))
          .flatMap(data => Option(deserialize[ServerMetadata](data)))
      }.collect { case Some(m) => m }.toSet

      val joined = children.diff(knownServers)
      val left = knownServers.diff(children)

      joined.foreach { server =>
        allocator.onServerJoin(server)
        info(s"Server joined: ${server.address}")
      }

      left.foreach { server =>
        allocator.onServerLeave(server)
        info(s"Server left: ${server.address}")
      }

      knownServers = children
    } catch {
      case e: Exception =>
        warn(s"Error syncing server registrations from ZooKeeper", e)
    }
  }

  def servers(): Seq[ServerMetadata] = synchronized {
    knownServers.toSeq.sortBy(_.key)
  }

  def getSelf: Option[ServerMetadata] = synchronized {
    self
  }

  def stop(): Unit = synchronized {
    node.foreach { n =>
      try {
        n.close()
      } catch {
        case e: Exception => warn("Error closing persistent node", e)
      }
    }
    syncServers()
    cache.foreach { c =>
      try {
        c.close()
      } catch {
        case e: Exception => warn("Error closing cache", e)
      }
    }
    cache = None
    node = None
    knownServers = Set.empty
  }
}
