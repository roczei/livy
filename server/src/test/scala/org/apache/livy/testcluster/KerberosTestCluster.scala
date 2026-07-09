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

package org.apache.livy.testcluster

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation

/**
 * In-process MiniKdc for Maven test scope.
 *
 * Follows the Apache Hadoop [[https://github.com/apache/hadoop hadoop-minikdc]] test pattern.
 * Secure HDFS/YARN coverage uses the embedded mini cluster in integration-test.
 */
class KerberosTestCluster(val krbInstance: String = "localhost") {

  private var kdc: MiniKdc = _
  private var _realm: String = _
  private var _keytab: File = _
  private var _krb5Conf: File = _
  private var _livyPrincipal: String = _
  val proxyUser: String = "proxy"

  def realm: String = _realm
  def keytab: File = _keytab
  def krb5Conf: File = _krb5Conf
  def livyPrincipal: String = _livyPrincipal

  def hadoopConf: Configuration = {
    val conf = new Configuration(false)
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
    conf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true)
    conf.set("hadoop.proxyuser.livy.users", "*")
    conf.set("hadoop.proxyuser.livy.hosts", "*")
    conf
  }

  def start(): Unit = {
    val workDir = new File(sys.props("java.io.tmpdir"), s"livy-krb-test-${System.nanoTime()}")
    assert(workDir.mkdirs(), s"Cannot create ${workDir.getAbsolutePath}")

    val kdcConf = MiniKdc.createConf()
    kdc = new MiniKdc(kdcConf, new File(workDir, "kdc"))
    kdc.start()
    _realm = kdc.getRealm

    _keytab = new File(workDir, "test.keytab")
    kdc.createPrincipal(
      _keytab,
      s"livy/$krbInstance",
      s"proxy/$krbInstance")

    _krb5Conf = new File(workDir, "krb5.conf")
    if (!kdc.getKrb5conf.renameTo(_krb5Conf)) {
      val in = new java.io.FileInputStream(kdc.getKrb5conf)
      try {
        val out = new java.io.FileOutputStream(_krb5Conf)
        try {
          val buf = new Array[Byte](4096)
          var n = in.read(buf)
          while (n > 0) {
            out.write(buf, 0, n)
            n = in.read(buf)
          }
        } finally {
          out.close()
        }
      } finally {
        in.close()
      }
    }
    System.setProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF, _krb5Conf.getAbsolutePath)

    _livyPrincipal = s"livy/$krbInstance@${_realm}"

    val conf = hadoopConf
    SecurityUtil.setAuthenticationMethod(
      UserGroupInformation.AuthenticationMethod.KERBEROS, conf)
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(_livyPrincipal, _keytab.getAbsolutePath)
  }

  def stop(): Unit = {
    if (kdc != null) {
      kdc.stop()
      kdc = null
    }
  }
}
