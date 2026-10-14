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

import java.io.ByteArrayInputStream
import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.io.Source

import org.apache.livy.LivyConf
import org.apache.livy.Logging
import org.apache.livy.server.recovery.SessionStore

class ThriftHttpForwardingFilter(
    allocator: Allocator,
    self: () => Option[ServerMetadata],
    sessionStore: SessionStore,
    livyConf: LivyConf)
  extends Filter with Logging {

  private val thriftHttpPathPattern = """^/thrift/(.+)$""".r

  override def init(filterConfig: FilterConfig): Unit = {}

  override def doFilter(request: ServletRequest,
                        response: ServletResponse,
                        chain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]

    val servletPath = Option(httpRequest.getServletPath).getOrElse("")
    val pathInfo = Option(httpRequest.getPathInfo).getOrElse("")
    val fullPath = servletPath + pathInfo

    thriftHttpPathPattern.findFirstMatchIn(fullPath) match {
      case Some(_) =>
        val sessionId = extractSessionIdFromThriftRequest(httpRequest)
        sessionId match {
          case Some(id) =>
            allocator.findServer(id) match {
              case Some(owner) =>
                self() match {
                  case Some(me) if me.key == owner.key =>
                    info(s"Thrift request for session $id is owned by this node, " +
                      "processing locally")
                    chain.doFilter(request, response)
                  case Some(_) =>
                    info(s"Thrift request for session $id should be forwarded to ${owner.address}")
                    forwardToOwner(httpResponse, owner, id)
                  case None =>
                    handleNoSelf(httpResponse, chain, request)
                }
              case None =>
                handleNoOwner(httpResponse, chain, request)
            }
          case None =>
            chain.doFilter(request, response)
        }
      case None =>
        chain.doFilter(request, response)
    }
  }

  private def extractSessionIdFromThriftRequest(req: HttpServletRequest): Option[Int] = {
    Option(req.getHeader("X-Livy-Session-Id")).flatMap { sessionIdStr =>
      if (sessionIdStr.forall(_.isDigit)) Some(sessionIdStr.toInt) else None
    }
  }

  private def forwardToOwner(httpResponse: HttpServletResponse,
                             owner: ServerMetadata,
                             sessionId: Int): Unit = {
    httpResponse.sendError(HttpServletResponse.SC_TEMPORARY_REDIRECT,
      s"Session $sessionId is owned by ${owner.address}. " +
        "Forward request to that server or use REST API for automatic routing.")
  }

  private def handleNoSelf(httpResponse: HttpServletResponse,
                           chain: FilterChain,
                           request: ServletRequest): Unit = {
    if (livyConf.getBoolean(LivyConf.HA_ROUTING_FAIL_OPEN)) {
      chain.doFilter(request, httpResponse)
    } else {
      httpResponse.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
        "Server not yet registered in cluster")
    }
  }

  private def handleNoOwner(httpResponse: HttpServletResponse,
                            chain: FilterChain,
                            request: ServletRequest): Unit = {
    if (livyConf.getBoolean(LivyConf.HA_ROUTING_FAIL_OPEN)) {
      chain.doFilter(request, httpResponse)
    } else {
      httpResponse.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
        "No servers available in cluster")
    }
  }

  override def destroy(): Unit = {}
}
