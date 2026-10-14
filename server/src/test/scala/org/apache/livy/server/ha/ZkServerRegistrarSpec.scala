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

import scala.collection.mutable

import org.apache.curator.test.TestingServer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.recovery.ZooKeeperManager

class ZkServerRegistrarSpec extends AnyFunSpec with LivyBaseUnitTestSuite with BeforeAndAfterAll {

  private var testingServer: TestingServer = _
  private var zkAddress: String = _

  override def beforeAll(): Unit = {
    testingServer = new TestingServer()
    zkAddress = testingServer.getConnectString
  }

  override def afterAll(): Unit = {
    if (testingServer != null) {
      testingServer.close()
    }
  }

  it("should register and list servers") {
    val conf = new LivyConf()
    conf.set(LivyConf.ZOOKEEPER_URL, zkAddress)
    conf.set(LivyConf.HA_MULTI_ACTIVE_ENABLED, true)

    val zkManager = new ZooKeeperManager(conf)
    zkManager.start()

    try {
      val allocator = new ConsistentHashingAllocator(conf)
      val registrar = new ZkServerRegistrar(zkManager.curator, conf, allocator)

      val server1 = ServerMetadata("host1", 8998, "uuid1")
      registrar.register(server1)

      Thread.sleep(100)

      val servers = registrar.servers()
      assert(servers.length === 1)
      assert(servers.head === server1)

      registrar.stop()
    } finally {
      zkManager.stop()
    }
  }

  it("should sync servers between two registrars") {
    val conf = new LivyConf()
    conf.set(LivyConf.ZOOKEEPER_URL, zkAddress)
    conf.set(LivyConf.HA_MULTI_ACTIVE_ENABLED, true)

    val zkManager1 = new ZooKeeperManager(conf)
    zkManager1.start()

    val zkManager2 = new ZooKeeperManager(conf)
    zkManager2.start()

    try {
      val allocator1 = new ConsistentHashingAllocator(conf)
      val registrar1 = new ZkServerRegistrar(zkManager1.curator, conf, allocator1)

      val allocator2 = new ConsistentHashingAllocator(conf)
      val registrar2 = new ZkServerRegistrar(zkManager2.curator, conf, allocator2)

      val server1 = ServerMetadata("host1", 8998, "uuid1")
      val server2 = ServerMetadata("host2", 8999, "uuid2")

      registrar1.register(server1)
      Thread.sleep(200)

      registrar2.register(server2)
      Thread.sleep(200)

      val servers1 = registrar1.servers()
      val servers2 = registrar2.servers()

      assert(servers1.length === 2)
      assert(servers2.length === 2)
      assert(servers1.map(_.key).toSet === servers2.map(_.key).toSet)

      registrar1.stop()
      Thread.sleep(200)

      val servers2After = registrar2.servers()
      assert(servers2After.length === 1)
      assert(servers2After.head === server2)

      registrar2.stop()
    } finally {
      zkManager1.stop()
      zkManager2.stop()
    }
  }

  it("should call allocator callbacks") {
    val conf = new LivyConf()
    conf.set(LivyConf.ZOOKEEPER_URL, zkAddress)
    conf.set(LivyConf.HA_MULTI_ACTIVE_ENABLED, true)

    val zkManager = new ZooKeeperManager(conf)
    zkManager.start()

    try {
      val joinedServers = mutable.Set[ServerMetadata]()
      val leftServers = mutable.Set[ServerMetadata]()

      val mockAllocator = new Allocator {
        def findServer(sessionId: Int): Option[ServerMetadata] = None
        def allocateServer(sessionId: Int): ServerMetadata =
          throw new UnsupportedOperationException()
        def onServerJoin(server: ServerMetadata): Unit = joinedServers += server
        def onServerLeave(server: ServerMetadata): Unit = leftServers += server
      }

      val registrar = new ZkServerRegistrar(zkManager.curator, conf, mockAllocator)

      val server1 = ServerMetadata("host1", 8998, "uuid1")
      registrar.register(server1)
      Thread.sleep(200)

      assert(joinedServers.contains(server1))

      registrar.stop()
      Thread.sleep(200)

      assert(leftServers.contains(server1))
    } finally {
      zkManager.stop()
    }
  }
}
