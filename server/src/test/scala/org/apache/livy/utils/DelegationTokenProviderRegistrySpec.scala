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

import org.scalatest.FunSpec

import org.apache.livy.LivyConf

class DelegationTokenProviderRegistrySpec extends FunSpec {
  describe("DelegationTokenProviderRegistry") {
    it("should register built-in providers") {
      val providers = DelegationTokenProviderRegistry.providersFor(new LivyConf())
      assert(providers.contains("hdfs"))
      assert(providers.contains("hive"))
      assert(providers.contains("hbase"))
      assert(providers.contains("kafka"))
    }

    it("should parse provider class mappings from livy.conf") {
      val conf = new LivyConf()
        .set(LivyConf.DELEGATION_TOKEN_PROVIDERS,
          s"${TestDelegationTokenProvider.PROVIDER_NAME}=" +
            "org.apache.livy.utils.TestDelegationTokenProvider")
      val entries = DelegationTokenProviderRegistry.configuredProviderEntries(conf)
      assert(entries === Seq(
        TestDelegationTokenProvider.PROVIDER_NAME ->
          "org.apache.livy.utils.TestDelegationTokenProvider"))
    }

    it("should load custom provider from config") {
      val conf = new LivyConf()
        .set(LivyConf.DELEGATION_TOKEN_PROVIDERS,
          s"${TestDelegationTokenProvider.PROVIDER_NAME}=" +
            "org.apache.livy.utils.TestDelegationTokenProvider")
        .set(LivyConf.DELEGATION_TOKEN_SERVICES, TestDelegationTokenProvider.PROVIDER_NAME)
      val providers = DelegationTokenProviderRegistry.providersFor(conf)
      assert(providers(TestDelegationTokenProvider.PROVIDER_NAME).getClass ===
        classOf[TestDelegationTokenProvider])
    }

    it("should ignore invalid provider entries") {
      val conf = new LivyConf()
        .set(LivyConf.DELEGATION_TOKEN_PROVIDERS, "invalid-entry-without-equals")
      assert(DelegationTokenProviderRegistry.configuredProviderEntries(conf).isEmpty)
    }
  }
}
