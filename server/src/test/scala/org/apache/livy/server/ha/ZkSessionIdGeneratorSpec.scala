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

class ZkSessionIdGeneratorSpec extends AnyFunSpec with LivyBaseUnitTestSuite
    with BeforeAndAfterAll {

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

  it("should generate unique ids") {
    val conf = new LivyConf()
    conf.set(LivyConf.ZOOKEEPER_URL, zkAddress)

    val zkManager = new ZooKeeperManager(conf)
    zkManager.start()

    try {
      val gen = new ZkSessionIdGenerator(zkManager.curator, "/test/session-ids/interactive", 0)
      gen.initialize()

      val ids = (1 to 10).map(_ => gen.nextId())
      assert(ids.length === ids.toSet.size, "All ids should be unique")
      assert(ids === ids.sorted, "Ids should be in increasing order")
    } finally {
      zkManager.stop()
    }
  }

  it("should respect seed value") {
    val conf = new LivyConf()
    conf.set(LivyConf.ZOOKEEPER_URL, zkAddress)

    val zkManager = new ZooKeeperManager(conf)
    zkManager.start()

    try {
      val gen = new ZkSessionIdGenerator(zkManager.curator, "/test/session-ids/batch", 100)
      gen.initialize()

      val firstId = gen.nextId()
      assert(firstId === 100, s"First id should be 100, got $firstId")
    } finally {
      zkManager.stop()
    }
  }

  it("should not reset existing counter on initialize") {
    val conf = new LivyConf()
    conf.set(LivyConf.ZOOKEEPER_URL, zkAddress)

    val zkManager1 = new ZooKeeperManager(conf)
    zkManager1.start()

    try {
      val gen1 = new ZkSessionIdGenerator(zkManager1.curator, "/test/session-ids/test", 0)
      gen1.initialize()

      val id1 = gen1.nextId()
      assert(id1 === 0)

      val id2 = gen1.nextId()
      assert(id2 === 1)

      val zkManager2 = new ZooKeeperManager(conf)
      zkManager2.start()

      try {
        val gen2 = new ZkSessionIdGenerator(zkManager2.curator, "/test/session-ids/test", 0)
        gen2.initialize()

        val id3 = gen2.nextId()
        assert(id3 === 2, s"Should continue from 2, not reset to 0. Got $id3")
      } finally {
        zkManager2.stop()
      }
    } finally {
      zkManager1.stop()
    }
  }
}
