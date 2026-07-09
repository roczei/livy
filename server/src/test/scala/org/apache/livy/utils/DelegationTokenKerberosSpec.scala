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
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import org.apache.livy.LivyConf
import org.apache.livy.testcluster.KerberosTestCluster

/**
 * Kerberos delegation token unit tests using in-process MiniKdc (Apache Hadoop pattern).
 *
 * Runs in Maven test scope (`mvn test -pl server`) without an external cluster.
 * Secure HDFS/YARN end-to-end coverage is provided by the embedded mini cluster ITs.
 */
class DelegationTokenKerberosSpec extends FunSpec with BeforeAndAfterAll {

  private var cluster: KerberosTestCluster = _

  override def beforeAll(): Unit = {
    cluster = new KerberosTestCluster()
    cluster.start()
  }

  override def afterAll(): Unit = {
    if (cluster != null) {
      cluster.stop()
      cluster = null
    }
  }

  private def krbLivyConf(services: String): LivyConf = {
    val conf = new LivyConf()
      .set(LivyConf.IMPERSONATION_ENABLED, true)
      .set(LivyConf.DELEGATION_TOKEN_RENEWAL_ENABLED, true)
      .set(LivyConf.DELEGATION_TOKEN_SERVICES, services)
      .set(LivyConf.LAUNCH_KERBEROS_PRINCIPAL, cluster.livyPrincipal)
      .set(LivyConf.LAUNCH_KERBEROS_KEYTAB, cluster.keytab.getAbsolutePath)
    conf.hadoopConf.addResource(cluster.hadoopConf)
    conf
  }

  describe("DelegationTokenManager with KerberosTestCluster") {
    it("should enable Kerberos security in the test JVM") {
      assert(UserGroupInformation.isSecurityEnabled)
    }

    it("should enable renewal for proxy-user sessions") {
      val conf = krbLivyConf("hdfs")
      assert(DelegationTokenManager.isEnabled(conf, Some(cluster.proxyUser)))
      assert(!DelegationTokenManager.isEnabled(conf, None))
    }

    it("should obtain HBase delegation tokens via test provider") {
      val conf = krbLivyConf("hbase")
      val creds = DelegationTokenManager.obtainTokens(cluster.proxyUser, conf)
      assert(creds.getToken(new Text("HBASE_DELEGATION_TOKEN")) != null)
    }

    it("should obtain Kafka delegation tokens via test provider") {
      val conf = krbLivyConf("kafka")
      val creds = DelegationTokenManager.obtainTokens(cluster.proxyUser, conf)
      assert(creds.getToken(new Text("KAFKA_DELEGATION_TOKEN")) != null)
    }

    it("should obtain tokens for hbase,kafka together") {
      val conf = krbLivyConf("hbase,kafka")
      val creds = DelegationTokenManager.obtainTokens(cluster.proxyUser, conf)
      assert(creds.getToken(new Text("HBASE_DELEGATION_TOKEN")) != null)
      assert(creds.getToken(new Text("KAFKA_DELEGATION_TOKEN")) != null)
    }

    it("should start SessionDelegationTokenRenewer in batch and interactive mode") {
      val conf = krbLivyConf("hbase,kafka")
      val batchRenewer = new SessionDelegationTokenRenewer(1, cluster.proxyUser, conf, true)
      val interactiveRenewer = new SessionDelegationTokenRenewer(2, cluster.proxyUser, conf, false)
      try {
        val pushed = new java.util.concurrent.atomic.AtomicBoolean(false)
        batchRenewer.start()
        interactiveRenewer.start()
        batchRenewer.registerCredentialPusher(_ => pushed.set(true))
        interactiveRenewer.registerCredentialPusher(_ => pushed.set(true))
        assert(pushed.get())
      } finally {
        batchRenewer.stop()
        interactiveRenewer.stop()
      }
    }
  }
}
