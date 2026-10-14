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

import java.nio.ByteBuffer

import org.mockito.Mockito._
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.mockito.MockitoSugar

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.recovery.SessionStore

class ThriftBinaryForwardingFilterSpec extends AnyFunSpec with LivyBaseUnitTestSuite
    with MockitoSugar {

  it("should determine ownership of a session") {
    val allocator = mock[Allocator]
    val self = mock[ServerMetadata]
    val sessionStore = mock[SessionStore]
    val conf = new LivyConf()

    val filter = new ThriftBinaryForwardingFilter(
      allocator, () => Some(self), sessionStore, conf)

    when(self.key).thenReturn("host1:10000:uuid1")
    when(allocator.findServer(42)).thenReturn(Some(self))

    assert(filter.isOwner(42), "Should identify as owner of session 42")
  }

  it("should identify non-owned sessions") {
    val allocator = mock[Allocator]
    val self = mock[ServerMetadata]
    val owner = mock[ServerMetadata]
    val sessionStore = mock[SessionStore]
    val conf = new LivyConf()

    val filter = new ThriftBinaryForwardingFilter(
      allocator, () => Some(self), sessionStore, conf)

    when(self.key).thenReturn("host1:10000:uuid1")
    when(owner.key).thenReturn("host2:10000:uuid2")
    when(allocator.findServer(42)).thenReturn(Some(owner))

    assert(!filter.isOwner(42), "Should not identify as owner of session 42")
  }

  it("should find server for a session") {
    val allocator = mock[Allocator]
    val self = mock[ServerMetadata]
    val owner = mock[ServerMetadata]
    val sessionStore = mock[SessionStore]
    val conf = new LivyConf()

    val filter = new ThriftBinaryForwardingFilter(
      allocator, () => Some(self), sessionStore, conf)

    when(allocator.findServer(42)).thenReturn(Some(owner))

    assert(filter.getOwner(42).contains(owner), "Should find the owner server")
  }

  it("should handle binary Thrift buffer with session ID") {
    val allocator = mock[Allocator]
    val self = mock[ServerMetadata]
    val sessionStore = mock[SessionStore]
    val conf = new LivyConf()

    val filter = new ThriftBinaryForwardingFilter(
      allocator, () => Some(self), sessionStore, conf)

    val bb = ByteBuffer.allocate(100)
    bb.putInt(96)
    bb.putInt(0x80010001)
    bb.putInt(16)
    bb.put("ExecuteStatement".getBytes())
    bb.putInt(0)
    bb.put(Array[Byte](8, 0, 1, 0, 0, 0, 42))

    val result = filter.shouldForward(bb.array())
    assert(result.contains(42), "Should extract session ID from binary buffer")
  }

  it("should handle empty or invalid buffers gracefully") {
    val allocator = mock[Allocator]
    val self = mock[ServerMetadata]
    val sessionStore = mock[SessionStore]
    val conf = new LivyConf()

    val filter = new ThriftBinaryForwardingFilter(
      allocator, () => Some(self), sessionStore, conf)

    assert(filter.shouldForward(Array[Byte]()).isEmpty,
      "Should return None for empty buffer")
    assert(filter.shouldForward(Array[Byte](1, 2, 3)).isEmpty,
      "Should return None for short buffer")
  }

  it("should have Thrift binary address in ServerMetadata") {
    val server = ServerMetadata("host1", 8998, "uuid1", "http", "alive", Some(10000))
    assert(server.thriftBinaryAddress.contains("host1:10000"),
      "Should have Thrift binary address")

    val serverNoThrift = ServerMetadata("host1", 8998, "uuid1", "http", "alive")
    assert(serverNoThrift.thriftBinaryAddress.isEmpty,
      "Should have no Thrift binary address when not configured")
  }
}
