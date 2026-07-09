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
import org.scalatest.FunSpec

import org.apache.livy.LivyConf

class DelegationTokenManagerSpec extends FunSpec {
  describe("DelegationTokenManager") {
    it("should stay disabled without proxy user or security") {
      val conf = new LivyConf()
      assert(!DelegationTokenManager.isEnabled(conf, None))
      assert(!DelegationTokenManager.isEnabled(conf, Some("alice")))
    }

    it("should stay disabled when impersonation is off even with proxy user") {
      val conf = new LivyConf()
        .set(LivyConf.IMPERSONATION_ENABLED, false)
        .set(LivyConf.DELEGATION_TOKEN_RENEWAL_ENABLED, true)
      assert(!DelegationTokenManager.isEnabled(conf, Some("alice")))
    }

    it("should stay disabled when renewal is explicitly off") {
      val conf = new LivyConf()
        .set(LivyConf.IMPERSONATION_ENABLED, true)
        .set(LivyConf.DELEGATION_TOKEN_RENEWAL_ENABLED, false)
      assert(!DelegationTokenManager.isEnabled(conf, Some("alice")))
    }

    it("should round-trip credentials serialization") {
      val creds = new Credentials()
      creds.addToken(new Text("test"), new Token())
      val bytes = DelegationTokenManager.serialize(creds)
      val restored = DelegationTokenManager.deserialize(bytes)
      assert(restored.numberOfTokens() === creds.numberOfTokens())
    }

    it("should expose batch RPC bootstrap listener class name") {
      assert(DelegationTokenManager.DELEGATION_TOKEN_RPC_BOOTSTRAP_CLASS
        === "org.apache.livy.rsc.driver.DelegationTokenRpcBootstrap")
    }
  }
}
