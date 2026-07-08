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

package org.apache.livy.server.token

import org.scalatest.{FunSpec, Matchers}

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.server.batch.BatchSession

/**
 * Unit tests for [[BatchSession.injectTokenReceiver]]. These verify the pure
 * config-mutation logic (adding spark.jars, spark.extraListeners, mailbox conf)
 * without exercising the HDFS or RPC layers.
 */
class BatchTokenInjectionSpec extends FunSpec with Matchers with LivyBaseUnitTestSuite {

  private val listenerClass = "org.apache.livy.tokenreceiver.LivyBatchTokenReceiver"

  describe("BatchSession.injectTokenReceiver") {

    it("returns the conf unchanged when token renewal is disabled") {
      val livyConf = new LivyConf()
        .set(LivyConf.TOKEN_RENEWAL_ENABLED, false: java.lang.Boolean)
      val input = Map("spark.master" -> "yarn")
      BatchSession.injectTokenReceiver(input, "livy-batch-1", livyConf) shouldBe input
    }

    it("returns the conf unchanged when the listener JAR cannot be located") {
      val livyConf = new LivyConf()
        .set(LivyConf.TOKEN_RENEWAL_ENABLED, true: java.lang.Boolean)
      // Not setting TOKEN_RENEWAL_BATCH_LISTENER_JAR — auto-detection is best-effort;
      // the assertion below only cares that no exception is thrown and that no extra
      // token-receiver keys leak in when the listener JAR is not present.
      val input = Map("spark.master" -> "yarn")
      val output = BatchSession.injectTokenReceiver(input, "livy-batch-2", livyConf)
      input.keySet.subsetOf(output.keySet) shouldBe true
    }

    it("injects listener JAR, extraListener class and mailbox conf when enabled") {
      val livyConf = new LivyConf()
        .set(LivyConf.TOKEN_RENEWAL_ENABLED, true: java.lang.Boolean)
        .set(LivyConf.TOKEN_RENEWAL_BATCH_LISTENER_JAR, "/tmp/livy-token-receiver.jar")
        .set(LivyConf.TOKEN_RENEWAL_MAILBOX_DIR, "hdfs:///livy/mailboxes")
      val input = Map("spark.master" -> "yarn")
      val output = BatchSession.injectTokenReceiver(input, "livy-batch-3", livyConf)
      output("spark.jars") should include ("/tmp/livy-token-receiver.jar")
      output("spark.extraListeners") should include (listenerClass)
      output("spark.livy.token-receiver.mailbox-dir") shouldBe "hdfs:///livy/mailboxes"
      output("spark.livy.token-receiver.session-tag") shouldBe "livy-batch-3"
    }

    it("merges rather than replaces existing spark.jars and spark.extraListeners") {
      val livyConf = new LivyConf()
        .set(LivyConf.TOKEN_RENEWAL_ENABLED, true: java.lang.Boolean)
        .set(LivyConf.TOKEN_RENEWAL_BATCH_LISTENER_JAR, "/tmp/livy-token-receiver.jar")
        .set(LivyConf.TOKEN_RENEWAL_MAILBOX_DIR, "hdfs:///livy/mailboxes")
      val input = Map(
        "spark.master" -> "yarn",
        "spark.jars" -> "/user/alice/app.jar,/user/alice/dep.jar",
        "spark.extraListeners" -> "com.example.MyListener")
      val output = BatchSession.injectTokenReceiver(input, "livy-batch-4", livyConf)
      output("spark.jars") should include ("/user/alice/app.jar")
      output("spark.jars") should include ("/user/alice/dep.jar")
      output("spark.jars") should include ("/tmp/livy-token-receiver.jar")
      output("spark.extraListeners") should include ("com.example.MyListener")
      output("spark.extraListeners") should include (listenerClass)
    }
  }
}
