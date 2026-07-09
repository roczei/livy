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

package org.apache.livy.test

import org.apache.livy.test.framework.BaseIntegrationTestSuite

/**
 * Integration tests for RPC-based delegation token renewal.
 *
 * Matrix dimensions:
 *  - deploy mode: client, cluster
 *  - session type: interactive, batch
 *  - impersonation: none, proxy-user (Kerberos only)
 *  - services: hdfs, hive, hbase, kafka (configured via cluster.spec)
 *
 * Mini-cluster uses embedded MiniKdc (Apache Hadoop pattern) with Kerberos enabled by default.
 * Embedded services: HDFS, YARN, Hive Metastore, HBase, Kafka, Ozone (Maven test scope).
 */
class DelegationTokenIT extends BaseIntegrationTestSuite with DelegationTokenTestSupport {

  private val deployModes = Seq(
    ("client", DeployClient),
    ("cluster", DeployCluster))

  private val impersonationModes = Seq("no-impersonation", "impersonation")

  private val services = testServices

  // ---------------------------------------------------------------------------
  // Mini-cluster / non-Kerberos regression (HDFS, no impersonation)
  // ---------------------------------------------------------------------------

  deployModes.foreach { case (modeName, deployConf) =>
    test(s"IT-DT-mini-interactive-$modeName-no-impersonation-hdfs") {
      assumeServiceConfigured("hdfs")
      withInteractiveSession(deployConf, None) { s =>
        verifyNoFilesystemTokenPolling(s)
        verifyHdfsAccessInteractive(s)
      }
    }

    test(s"IT-DT-mini-batch-$modeName-no-impersonation-hdfs") {
      assumeServiceConfigured("hdfs")
      verifyBatchSuccess(deployConf, None)
    }
  }

  // ---------------------------------------------------------------------------
  // Kerberos: interactive sessions
  // ---------------------------------------------------------------------------

  deployModes.foreach { case (modeName, deployConf) =>
    impersonationModes.foreach { impName =>
      services.foreach { service =>
        test(s"IT-DT-krb-interactive-$modeName-$impName-$service") {
          val proxy = impersonationProxy(impName)
          assumeKerberosForImpersonation(proxy)
          assumeServiceConfigured(service)

          withInteractiveSession(deployConf, proxy) { s =>
            verifyNoFilesystemTokenPolling(s)
            verifyServiceAccessInteractive(s, service)
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Kerberos: batch sessions
  // ---------------------------------------------------------------------------

  deployModes.foreach { case (modeName, deployConf) =>
    impersonationModes.foreach { impName =>
      services.foreach { service =>
        test(s"IT-DT-krb-batch-$modeName-$impName-$service") {
          val proxy = impersonationProxy(impName)
          assumeKerberosForImpersonation(proxy)
          assumeServiceConfigured(service)

          // batch-dt-services.py checks only services listed in spark.livy.test.services;
          // narrow to the service under test.
          val narrowedConf = Map("spark.livy.test.services" -> service)
          withBatchSession(
              deployConf,
              proxy,
              script = "batch-dt-services.py",
              extraConf = narrowedConf) { batch =>
            batch.verifySessionState(org.apache.livy.sessions.SessionState.Success())
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Delegation token renewal: short-lived tokens + long-running Spark job
  // ---------------------------------------------------------------------------

  deployModes.foreach { case (modeName, deployConf) =>
    renewalServices.foreach { service =>
      test(s"IT-DT-renewal-batch-$modeName-impersonation-$service") {
        assume(kerberosEnabled, "Requires Kerberos mini cluster")
        assumeKerberosForImpersonation(proxyUser)
        assumeServiceConfigured(service)
        verifyRenewalBatch(deployConf, proxyUser, service)
      }
    }
  }

  deployModes.foreach { case (modeName, deployConf) =>
    Seq("hdfs", "ozone").foreach { service =>
      test(s"IT-DT-renewal-interactive-$modeName-impersonation-$service") {
        assume(kerberosEnabled, "Requires Kerberos mini cluster")
        assumeKerberosForImpersonation(proxyUser)
        if (service == "ozone") assume(ozoneConfigured) else assumeServiceConfigured(service)

        withInteractiveSession(deployConf, proxyUser, extraConf = renewalSparkConf) { s =>
          verifyNoFilesystemTokenPolling(s)
          verifyRenewalInteractive(s, service)
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Legacy smoke tests (backward compatible names)
  // ---------------------------------------------------------------------------

  test("IT-DT-01: interactive YARN session does not configure filesystem token polling") {
    withInteractiveSession(DeployCluster, None) { s =>
      verifyNoFilesystemTokenPolling(s)
      s.run("1+1").verifyResult("res0: Int = 2\n")
    }
  }

  test("IT-DT-02: batch YARN session completes successfully") {
    verifyBatchSuccess(DeployCluster, None, script = "batch-dt-test.py")
  }

  test("IT-DT-K01: interactive proxy-user session runs HDFS job on YARN") {
    assume(kerberosEnabled, "Requires Kerberos mini cluster (kerberos.enabled=true)")
    assumeServiceConfigured("hdfs")

    withInteractiveSession(DeployCluster, proxyUser) { s =>
      verifyNoFilesystemTokenPolling(s)
      verifyHdfsAccessInteractive(s)
    }
  }

  test("IT-DT-K03: batch cluster-mode proxy-user session accesses HDFS") {
    assume(kerberosEnabled, "Requires Kerberos mini cluster")
    assumeServiceConfigured("hdfs")

    verifyBatchSuccess(DeployCluster, proxyUser, script = "batch-dt-krb.py")
  }

  // ---------------------------------------------------------------------------
  // Ozone OFS: embedded MiniOzoneCluster + auto-discovered delegation tokens
  // ---------------------------------------------------------------------------

  test("IT-DT-O01: interactive cluster-mode proxy-user accesses Ozone via ofs://") {
    assume(kerberosEnabled, "Requires Kerberos mini cluster")
    assume(ozoneConfigured, "Requires embedded MiniOzoneCluster")

    withInteractiveSession(
        DeployCluster,
        proxyUser,
        extraConf = Map(
          "spark.livy.test.services" -> "hdfs,ozone",
          "spark.files" -> s"${ozoneTestPath}/dt-marker.txt")) { s =>
      verifyNoFilesystemTokenPolling(s)
      verifyOzoneAccessInteractive(s)
    }
  }

  test("IT-DT-O02: batch cluster-mode proxy-user auto-discovers Ozone ofs:// paths") {
    assume(kerberosEnabled, "Requires Kerberos mini cluster")
    assume(ozoneConfigured, "Requires embedded MiniOzoneCluster")

    withBatchSession(
        DeployCluster,
        proxyUser,
        script = "batch-dt-services.py",
        extraConf = Map(
          "spark.livy.test.services" -> "hdfs,ozone",
          "spark.files" -> s"${ozoneTestPath}/dt-marker.txt")) { batch =>
      batch.verifySessionState(org.apache.livy.sessions.SessionState.Success())
    }
  }
}
