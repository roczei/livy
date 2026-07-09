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

package org.apache.livy.test.framework

import java.io.{File, FileInputStream, FileOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration

/**
 * Hadoop-style Kerberos test cluster setup using [[MiniKdc]] (test scope only).
 */
private[framework] object KerberosMiniClusterSupport extends MiniClusterUtils {

  val KERBEROS_ENABLED_KEY = "kerberos.enabled"
  val PROXY_USER_KEY = "livy.test.proxyUser"

  case class KerberosClusterInfo(
      kdc: MiniKdc,
      realm: String,
      krbInstance: String,
      keytabFile: File,
      krb5Conf: File,
      livyPrincipal: String,
      proxyUser: String)

  def isKerberosEnabled(config: Map[String, String]): Boolean = {
    config.get(KERBEROS_ENABLED_KEY).forall(_.toBoolean)
  }

  def startKerberos(workDir: File, config: Map[String, String]): KerberosClusterInfo = {
    val kdcWorkDir = new File(workDir, "kdc")
    val kdcConf = MiniKdc.createConf()
    val kdc = new MiniKdc(kdcConf, kdcWorkDir)
    kdc.start()

    val krbInstance = config.getOrElse("kerberos.instance", "localhost")
    val realm = kdc.getRealm
    val keytabFile = new File(workDir, "test.keytab")

    kdc.createPrincipal(
      keytabFile,
      s"hdfs/$krbInstance",
      s"HTTP/$krbInstance",
      s"yarn/$krbInstance",
      s"livy/$krbInstance",
      s"proxy/$krbInstance",
      s"om/$krbInstance",
      s"scm/$krbInstance",
      s"hive/$krbInstance",
      s"hbase/$krbInstance",
      s"kafka/$krbInstance")

    val krb5Conf = new File(kdcWorkDir, "krb5.conf")
    if (!kdc.getKrb5conf.renameTo(krb5Conf)) {
      copyFile(kdc.getKrb5conf, krb5Conf)
    }
    patchKrb5ConfForLocalKdc(krb5Conf)
    System.setProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF, krb5Conf.getAbsolutePath)

    val proxyUser = config.getOrElse(PROXY_USER_KEY, "proxy")
    KerberosClusterInfo(
      kdc = kdc,
      realm = realm,
      krbInstance = krbInstance,
      keytabFile = keytabFile,
      krb5Conf = krb5Conf,
      livyPrincipal = s"livy/$krbInstance@$realm",
      proxyUser = proxyUser)
  }

  def stopKerberos(info: Option[KerberosClusterInfo]): Unit = {
    info.foreach(_.kdc.stop())
  }

  def krb5JvmArg(krb5Conf: File): String =
    s"-D${MiniKdc.JAVA_SECURITY_KRB5_CONF}=${krb5Conf.getAbsolutePath}"

  def writeSecureConfigs(configDir: File, info: KerberosClusterInfo): Unit = {
    val hdfsPrincipal = s"hdfs/${info.krbInstance}@${info.realm}"
    val yarnPrincipal = s"yarn/${info.krbInstance}@${info.realm}"
    val spnegoPrincipal = s"HTTP/${info.krbInstance}@${info.realm}"
    val keytab = info.keytabFile.getAbsolutePath
    val tokenMaxLifetimeMs = TestDelegationTokenDefaults.MaxLifetimeMs
    val tokenRenewIntervalMs = TestDelegationTokenDefaults.ExpiryTimeMs

    val coreConf = new Configuration(false)
    coreConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
    coreConf.setBoolean(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true)
    coreConf.set("hadoop.proxyuser.livy.users", "*")
    coreConf.set("hadoop.proxyuser.livy.hosts", "*")
    // Prevent ShellBasedUnixGroupsMapping from failing on OS users that don't exist
    // (hdfs, yarn, livy, hive, hbase, kafka are KDC principals, not real OS accounts).
    coreConf.set("hadoop.user.group.static.mapping.overrides",
      "hdfs=supergroup;yarn=supergroup;livy=supergroup;HTTP=supergroup;" +
      "hive=supergroup;hbase=supergroup;kafka=supergroup;mapred=supergroup")
    coreConf.setLong("hive.cluster.delegation.token.max-lifetime", tokenMaxLifetimeMs)
    coreConf.setLong("hive.cluster.delegation.token.renew-interval", tokenRenewIntervalMs)
    coreConf.setLong("hive.cluster.delegation.token.gc-interval", 5L * 1000)
    coreConf.setLong("hbase.auth.token.max.lifetime", tokenMaxLifetimeMs)
    coreConf.setLong("hbase.auth.key.update.interval", tokenRenewIntervalMs)
    coreConf.setLong(TestDelegationTokenDefaults.MaxLifetimeKey, tokenMaxLifetimeMs)
    coreConf.setLong(TestDelegationTokenDefaults.ExpiryTimeKey, tokenRenewIntervalMs)
    saveConfig(coreConf, new File(configDir, "core-site.xml"))

    val hdfsConf = new Configuration(false)
    hdfsConf.addResource(new Path(new File(configDir, "core-site.xml").toURI))
    hdfsConf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal)
    hdfsConf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytab)
    hdfsConf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal)
    hdfsConf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytab)
    hdfsConf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal)
    hdfsConf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY, keytab)
    hdfsConf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true)
    hdfsConf.setBoolean("ignore.secure.ports.for.testing", true)
    hdfsConf.set("dfs.data.transfer.protection", "authentication")
    hdfsConf.setLong(
      DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_MAX_LIFETIME_KEY, tokenMaxLifetimeMs)
    hdfsConf.setLong(
      DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_RENEW_INTERVAL_KEY, tokenRenewIntervalMs)
    saveConfig(hdfsConf, new File(configDir, "hdfs-site.xml"))

    val yarnConf = new YarnConfiguration()
    yarnConf.clear()
    yarnConf.addResource(new Path(new File(configDir, "core-site.xml").toURI))
    yarnConf.set(YarnConfiguration.RM_PRINCIPAL, yarnPrincipal)
    yarnConf.set(YarnConfiguration.RM_KEYTAB, keytab)
    yarnConf.set(YarnConfiguration.NM_PRINCIPAL, yarnPrincipal)
    yarnConf.set(YarnConfiguration.NM_KEYTAB, keytab)
    yarnConf.set("yarn.node-attribute.fs-store.root-dir", "/yarn/nodeattributes")
    yarnConf.set("yarn.node-labels.fs-store.root-dir", "/yarn/nodelabels")
    saveConfig(yarnConf, new File(configDir, "yarn-site.xml"))
  }

  def loginLivyUser(info: KerberosClusterInfo): Unit = {
    val conf = new Configuration(false)
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
    SecurityUtil.setAuthenticationMethod(
      UserGroupInformation.AuthenticationMethod.KERBEROS, conf)
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(info.livyPrincipal, info.keytabFile.getAbsolutePath)
  }

  def clusterProperties(info: KerberosClusterInfo): Map[String, String] = Map(
    "cluster.type" -> "mini",
    "authScheme" -> "kerberos",
    "user" -> "livy",
    PROXY_USER_KEY -> info.proxyUser,
    "principal" -> info.livyPrincipal,
    "keytabPath" -> info.keytabFile.getAbsolutePath,
    "delegation.token.services" -> "hdfs,hive,hbase,kafka",
    TestDelegationTokenDefaults.MaxLifetimeKey -> TestDelegationTokenDefaults.MaxLifetimeMs.toString,
    TestDelegationTokenDefaults.ExpiryTimeKey -> TestDelegationTokenDefaults.ExpiryTimeMs.toString)

  def livyKerberosProperties(info: KerberosClusterInfo): Map[String, String] = Map(
    LivyConfKeys.IMPERSONATION_ENABLED -> "true",
    LivyConfKeys.DELEGATION_TOKEN_RENEWAL_ENABLED -> "true",
    LivyConfKeys.DELEGATION_TOKEN_RENEWAL_INTERVAL -> "5s",
    LivyConfKeys.LAUNCH_KERBEROS_PRINCIPAL -> info.livyPrincipal,
    LivyConfKeys.LAUNCH_KERBEROS_KEYTAB -> info.keytabFile.getAbsolutePath,
    LivyConfKeys.LAUNCH_KERBEROS_KEYTAB_LOGIN_ONLY -> "true",
    LivyConfKeys.DELEGATION_TOKEN_SERVICES -> "hdfs,hive,hbase,kafka")

  private def patchKrb5ConfForLocalKdc(krb5Conf: File): Unit = {
    // MiniKdc writes "localhost"; macOS kinit may prefer IPv6 ::1 while the KDC listens on
    // 127.0.0.1 only.
    val content = scala.io.Source.fromFile(krb5Conf, "UTF-8").mkString
    val patched = content
      .replace("kdc = localhost:", "kdc = 127.0.0.1:")
      .replace("[libdefaults]", "[libdefaults]\n    dns_lookup_kdc = false\n    dns_lookup_realm = false")
    val out = new FileOutputStream(krb5Conf)
    try {
      out.write(patched.getBytes("UTF-8"))
    } finally {
      out.close()
    }
  }

  private def copyFile(src: File, dest: File): Unit = {
    val in = new FileInputStream(src)
    try {
      val out = new FileOutputStream(dest)
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

  private object LivyConfKeys {
    val IMPERSONATION_ENABLED = "livy.impersonation.enabled"
    val DELEGATION_TOKEN_RENEWAL_ENABLED = "livy.impersonation.delegation-token.renewal.enabled"
    val DELEGATION_TOKEN_RENEWAL_INTERVAL =
      "livy.impersonation.delegation-token.renewal.interval"
    val LAUNCH_KERBEROS_PRINCIPAL = "livy.server.launch.kerberos.principal"
    val LAUNCH_KERBEROS_KEYTAB = "livy.server.launch.kerberos.keytab"
    val LAUNCH_KERBEROS_KEYTAB_LOGIN_ONLY = "livy.server.launch.kerberos.keytab-login-only"
    val DELEGATION_TOKEN_SERVICES = "livy.impersonation.delegation-token.services"
  }
}
