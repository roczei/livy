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

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger
import org.apache.curator.retry.RetryNTimes

import org.apache.livy.Logging

class ZkSessionIdGenerator(curator: CuratorFramework, path: String, seed: Int)
  extends Logging {

  private val counter = new DistributedAtomicInteger(curator, path, new RetryNTimes(5, 100))

  def initialize(): Unit = {
    try {
      counter.initialize(seed)
    } catch {
      case _: Exception =>
        info(s"Counter at $path already initialized, using existing value")
    }
  }

  def nextId(): Int = {
    counter.increment().preValue()
  }

  def current(): Int = {
    counter.get().postValue()
  }
}
