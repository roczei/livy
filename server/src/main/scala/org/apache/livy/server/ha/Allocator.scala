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

import org.apache.livy.LivyConf

trait Allocator {
  def findServer(sessionId: Int): Option[ServerMetadata]
  def allocateServer(sessionId: Int): ServerMetadata
  def onServerJoin(server: ServerMetadata): Unit
  def onServerLeave(server: ServerMetadata): Unit
}

object Allocator {
  def create(livyConf: LivyConf): Allocator = {
    val className = livyConf.get(LivyConf.HA_ALLOCATOR_CLASS)
    val clazz = Class.forName(className).asInstanceOf[Class[Allocator]]
    val ctor = clazz.getConstructor(classOf[LivyConf])
    ctor.newInstance(livyConf)
  }
}
