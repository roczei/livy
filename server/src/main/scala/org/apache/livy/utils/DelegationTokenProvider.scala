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

package org.apache.livy.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.livy.LivyConf

/**
 * Pluggable delegation token provider. Implementations obtain service-specific tokens on behalf
 * of a proxy user and add them to a shared [[Credentials]] instance.
 */
trait DelegationTokenProvider {
  /** Short name referenced by livy.impersonation.delegation-token.services. */
  def name: String

  def obtainTokens(context: DelegationTokenProviderContext, credentials: Credentials): Unit
}

case class DelegationTokenProviderContext(
    proxyUser: String,
    proxyUgi: UserGroupInformation,
    renewer: String,
    hadoopConf: Configuration,
    livyConf: LivyConf,
    sessionFilesystemUris: Set[String] = Set.empty)
