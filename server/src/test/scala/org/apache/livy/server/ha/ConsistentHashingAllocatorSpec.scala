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

import org.scalatest.funspec.AnyFunSpec

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}

class ConsistentHashingAllocatorSpec extends AnyFunSpec with LivyBaseUnitTestSuite {

  it("should return None for empty ring") {
    val allocator = new ConsistentHashingAllocator(new LivyConf())
    assert(allocator.findServer(1) === None)
  }

  it("should throw when allocating from empty ring") {
    val allocator = new ConsistentHashingAllocator(new LivyConf())
    intercept[IllegalStateException] {
      allocator.allocateServer(1)
    }
  }

  it("should find owner after join") {
    val allocator = new ConsistentHashingAllocator(new LivyConf())
    val server1 = ServerMetadata("localhost", 8998, "uuid1")
    allocator.onServerJoin(server1)

    assert(allocator.findServer(1).contains(server1))
  }

  it("should distribute sessions across multiple servers") {
    val allocator = new ConsistentHashingAllocator(new LivyConf())
    val server1 = ServerMetadata("localhost", 8998, "uuid1")
    val server2 = ServerMetadata("localhost", 8999, "uuid2")
    val server3 = ServerMetadata("localhost", 9000, "uuid3")

    allocator.onServerJoin(server1)
    allocator.onServerJoin(server2)
    allocator.onServerJoin(server3)

    val owners = (1 to 300).map(id => allocator.findServer(id)).collect { case Some(s) => s }
    val distribution = owners.groupBy(_.key).mapValues(_.length)

    assert(distribution.size === 3)
    distribution.values.foreach { count =>
      assert(count > 0, s"Each server should have at least one session")
    }
  }

  it("should be deterministic for same server set") {
    val allocator = new ConsistentHashingAllocator(new LivyConf())
    val server1 = ServerMetadata("localhost", 8998, "uuid1")
    val server2 = ServerMetadata("localhost", 8999, "uuid2")

    allocator.onServerJoin(server1)
    allocator.onServerJoin(server2)

    val owners1 = (1 to 100).map(id => allocator.findServer(id))

    val allocator2 = new ConsistentHashingAllocator(new LivyConf())
    allocator2.onServerJoin(server2)
    allocator2.onServerJoin(server1)

    val owners2 = (1 to 100).map(id => allocator2.findServer(id))

    assert(owners1 === owners2, "Order of join should not matter")
  }

  it("should handle server leave correctly") {
    val allocator = new ConsistentHashingAllocator(new LivyConf())
    val server1 = ServerMetadata("localhost", 8998, "uuid1")
    val server2 = ServerMetadata("localhost", 8999, "uuid2")

    allocator.onServerJoin(server1)
    allocator.onServerJoin(server2)

    val ownersBefore = (1 to 100).map(id => allocator.findServer(id))

    allocator.onServerLeave(server2)

    val ownersAfter = (1 to 100).map(id => allocator.findServer(id))

    assert(ownersBefore !== ownersAfter, "Some sessions should be reassigned")
    assert(ownersAfter.forall(_.contains(server1)), "All should go to server1 after removal")
  }

  it("should be idempotent for join/leave") {
    val allocator = new ConsistentHashingAllocator(new LivyConf())
    val server1 = ServerMetadata("localhost", 8998, "uuid1")

    allocator.onServerJoin(server1)
    val owners1 = (1 to 100).map(id => allocator.findServer(id))

    allocator.onServerJoin(server1)
    val owners2 = (1 to 100).map(id => allocator.findServer(id))

    assert(owners1 === owners2, "Duplicate join should be idempotent")
  }
}
