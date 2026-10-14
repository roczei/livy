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

import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.livy.LivyConf
import org.apache.livy.Logging
import org.apache.livy.server.recovery.SessionStore

class SessionRoutingFilter(
    allocator: Allocator,
    self: () => Option[ServerMetadata],
    sessionStore: SessionStore,
    livyConf: LivyConf)
  extends Filter with Logging {

  private val sessionPathPattern = """^/(sessions|batches)/([^/]+)(.*)$""".r
  private val failOpen = livyConf.getBoolean(LivyConf.HA_ROUTING_FAIL_OPEN)

  override def init(filterConfig: FilterConfig): Unit = {}

  override def doFilter(request: ServletRequest,
                        response: ServletResponse,
                        chain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]

    val servletPath = Option(httpRequest.getServletPath).getOrElse("")
    val pathInfo = Option(httpRequest.getPathInfo).getOrElse("")
    val fullPath = servletPath + pathInfo
    val queryString = Option(httpRequest.getQueryString).map("?" + _).getOrElse("")

    sessionPathPattern.findFirstMatchIn(fullPath) match {
      case Some(m) =>
        val baseType = m.group(1)
        val idOrName = m.group(2)
        val restPath = m.group(3)

        val sessionId = if (idOrName.forall(_.isDigit)) {
          Some(idOrName.toInt)
        } else {
          resolveNameToId(baseType, idOrName)
        }

        sessionId match {
          case Some(id) =>
            allocator.findServer(id) match {
              case Some(owner) =>
                self() match {
                  case Some(me) if me.key == owner.key =>
                    chain.doFilter(request, response)
                  case Some(_) =>
                    redirect(httpResponse, owner, baseType, id, restPath, queryString)
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

  private def resolveNameToId(baseType: String, name: String): Option[Int] = {
    try {
      val sessionType = if (baseType == "sessions") "interactive" else "batch"
      sessionStore.getIdByName(sessionType, name)
    } catch {
      case e: Exception =>
        warn(s"Error resolving session name '$name' to id", e)
        None
    }
  }

  private def redirect(response: HttpServletResponse, owner: ServerMetadata,
      baseType: String, id: Int, restPath: String, queryString: String): Unit = {
    val location = s"${owner.url}/$baseType/$id$restPath$queryString"
    response.setStatus(307)
    response.setHeader("Location", location)
    response.flushBuffer()
  }

  private def handleNoOwner(response: HttpServletResponse, chain: FilterChain,
      request: ServletRequest): Unit = {
    if (failOpen) {
      chain.doFilter(request, response)
    } else {
      response.asInstanceOf[HttpServletResponse].sendError(
        HttpServletResponse.SC_SERVICE_UNAVAILABLE,
        "No servers available in cluster")
    }
  }

  private def handleNoSelf(response: HttpServletResponse, chain: FilterChain,
      request: ServletRequest): Unit = {
    chain.doFilter(request, response)
  }

  override def destroy(): Unit = {}
}
