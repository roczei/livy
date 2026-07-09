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

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.Token

import org.apache.livy.LivyConf

class TestDelegationTokenProvider extends DelegationTokenProvider {
  override val name: String = TestDelegationTokenProvider.PROVIDER_NAME

  override def obtainTokens(
      context: DelegationTokenProviderContext,
      credentials: Credentials): Unit = {
    credentials.addToken(new Text("test-provider-token"), new Token())
  }
}

object TestDelegationTokenProvider {
  val PROVIDER_NAME = "testprovider"
}
