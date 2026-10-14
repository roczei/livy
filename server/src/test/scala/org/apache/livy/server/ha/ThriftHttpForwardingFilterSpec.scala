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

import javax.servlet.{FilterChain, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.mockito.Mockito._
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.mockito.MockitoSugar

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.recovery.SessionStore

class ThriftHttpForwardingFilterSpec extends AnyFunSpec with LivyBaseUnitTestSuite
    with MockitoSugar {

  it("should forward to owner when not owning the session") {
    val allocator = mock[Allocator]
    val self = mock[ServerMetadata]
    val owner = mock[ServerMetadata]
    val sessionStore = mock[SessionStore]
    val conf = new LivyConf()
    conf.set(LivyConf.HA_ROUTING_FAIL_OPEN, true)

    val filter = new ThriftHttpForwardingFilter(
      allocator, () => Some(self), sessionStore, conf)

    val request = mock[HttpServletRequest]
    when(request.getServletPath).thenReturn("/thrift")
    when(request.getPathInfo).thenReturn("/cliService")
    when(request.getMethod).thenReturn("POST")
    when(request.getHeader("X-Livy-Session-Id")).thenReturn("42")

    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]

    when(self.key).thenReturn("host1:8998:uuid1")
    when(owner.key).thenReturn("host2:8999:uuid2")
    when(owner.url).thenReturn("http://host2:8999")
    when(allocator.findServer(42)).thenReturn(Some(owner))

    filter.doFilter(request, response, chain)

    // Filter should attempt to forward (actual forwarding would require HTTP setup)
    verify(allocator).findServer(42)
  }

  it("should pass through requests that own the session") {
    val allocator = mock[Allocator]
    val self = mock[ServerMetadata]
    val sessionStore = mock[SessionStore]
    val conf = new LivyConf()
    conf.set(LivyConf.HA_ROUTING_FAIL_OPEN, true)

    val filter = new ThriftHttpForwardingFilter(
      allocator, () => Some(self), sessionStore, conf)

    val request = mock[HttpServletRequest]
    when(request.getServletPath).thenReturn("/thrift")
    when(request.getPathInfo).thenReturn("/cliService")
    when(request.getHeader("X-Livy-Session-Id")).thenReturn("42")

    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]

    when(self.key).thenReturn("host1:8998:uuid1")
    when(allocator.findServer(42)).thenReturn(Some(self))

    filter.doFilter(request, response, chain)

    verify(chain).doFilter(request, response)
  }

  it("should pass through non-Thrift paths") {
    val allocator = mock[Allocator]
    val self = mock[ServerMetadata]
    val sessionStore = mock[SessionStore]
    val conf = new LivyConf()
    conf.set(LivyConf.HA_ROUTING_FAIL_OPEN, true)

    val filter = new ThriftHttpForwardingFilter(
      allocator, () => Some(self), sessionStore, conf)

    val request = mock[HttpServletRequest]
    when(request.getServletPath).thenReturn("/sessions")
    when(request.getPathInfo).thenReturn("/1")

    val response = mock[HttpServletResponse]
    val chain = mock[FilterChain]

    filter.doFilter(request, response, chain)

    verify(chain).doFilter(request, response)
    verify(allocator, never()).findServer(42)
  }
}
