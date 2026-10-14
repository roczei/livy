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

import org.apache.curator.test.TestingServer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.recovery.ZooKeeperManager

class MultiNodeRoutingSpec extends AnyFunSpec with LivyBaseUnitTestSuite with BeforeAndAfterAll {

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

  it("should route all sessions to the same owner consistently") {
    val conf = new LivyConf()
    conf.set(LivyConf.ZOOKEEPER_URL, zkAddress)

    val allocator = new ConsistentHashingAllocator(conf)

    // Add 3 servers to the ring
    val server1 = ServerMetadata("host1", 8998, "uuid1", "http", "RUNNING")
    val server2 = ServerMetadata("host2", 8999, "uuid2", "http", "RUNNING")
    val server3 = ServerMetadata("host3", 9000, "uuid3", "http", "RUNNING")

    allocator.onServerJoin(server1)
    allocator.onServerJoin(server2)
    allocator.onServerJoin(server3)

    // Same session ID should always map to same server
    val owner1 = allocator.findServer(42)
    val owner2 = allocator.findServer(42)
    val owner3 = allocator.findServer(42)

    assert(owner1 == owner2, "Same session should map to same owner")
    assert(owner2 == owner3, "Session owner should be consistent")
  }

  it("should move only affected sessions when a server leaves") {
    val conf = new LivyConf()
    conf.set(LivyConf.ZOOKEEPER_URL, zkAddress)

    val allocator = new ConsistentHashingAllocator(conf)

    val server1 = ServerMetadata("host1", 8998, "uuid1", "http", "RUNNING")
    val server2 = ServerMetadata("host2", 8999, "uuid2", "http", "RUNNING")

    allocator.onServerJoin(server1)
    allocator.onServerJoin(server2)

    // Find initial owners
    val (initialOwners1, initialOwners2) = (1 to 100).map { id =>
      (id, allocator.findServer(id).get)
    }.partition(_._2 == server1)

    assert(initialOwners1.nonEmpty && initialOwners2.nonEmpty, "Sessions should be distributed")

    // Remove server1
    allocator.onServerLeave(server1)

    // Check that only server1's sessions moved to server2
    val (movedSessions, unmoved) = (1 to 100).map { id =>
      (id, allocator.findServer(id).get)
    }.partition { case (id, owner) =>
      val wasOnServer1 = initialOwners1.exists(_._1 == id)
      wasOnServer1 && owner == server2
    }

    // All server1 sessions should now be on server2
    assert(movedSessions.length == initialOwners1.length,
      s"Expected ${initialOwners1.length} sessions to move, got ${movedSessions.length}")

    // Verify no server2 sessions moved
    val server2SessionsMovedCount = (1 to 100).count { id =>
      val wasOnServer2 = initialOwners2.exists(_._1 == id)
      val newOwner = allocator.findServer(id).get
      wasOnServer2 && newOwner != server2
    }
    assert(server2SessionsMovedCount == 0, "Server2 sessions should not move")
  }

  it("should find server for any session ID") {
    val conf = new LivyConf()
    conf.set(LivyConf.ZOOKEEPER_URL, zkAddress)

    val allocator = new ConsistentHashingAllocator(conf)

    val server1 = ServerMetadata("host1", 8998, "uuid1", "http", "RUNNING")
    allocator.onServerJoin(server1)

    // All IDs should find server1
    (0 to 1000).foreach { id =>
      assert(allocator.findServer(id).contains(server1),
        s"Session $id should find server1")
    }
  }
}
