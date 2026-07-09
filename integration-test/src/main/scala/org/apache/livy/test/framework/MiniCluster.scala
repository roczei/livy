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

import java.io._
import java.sql.DriverManager
import javax.servlet.http.HttpServletResponse

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.spark.launcher.SparkLauncher
import org.asynchttpclient.DefaultAsyncHttpClient
import org.scalatest.concurrent.Eventually._

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.client.common.TestUtils
import org.apache.livy.server.LivyServer

private class MiniClusterConfig(val config: Map[String, String]) {

  val nmCount = getInt("yarn.nm-count", 1)
  val localDirCount = getInt("yarn.local-dir-count", 1)
  val logDirCount = getInt("yarn.log-dir-count", 1)
  val dnCount = getInt("hdfs.dn-count", 1)

  private def getInt(key: String, default: Int): Int = {
    config.get(key).map(_.toInt).getOrElse(default)
  }

}

sealed abstract class MiniClusterBase extends MiniClusterUtils with Logging {

  def main(args: Array[String]): Unit = {
    val klass = getClass().getSimpleName()

    info(s"$klass is starting up.")

    val Array(configPath) = args
    val config = {
      val file = new File(s"$configPath/cluster.conf")
      val props = loadProperties(file)
      new MiniClusterConfig(props)
    }
    start(config, configPath)

    info(s"$klass running.")

    while (true) synchronized {
      wait()
    }
  }

  protected def start(config: MiniClusterConfig, configPath: String): Unit

}

object MiniHdfsMain extends MiniClusterBase {

  override protected def start(config: MiniClusterConfig, configPath: String): Unit = {
    val hadoopConf = new HdfsConfiguration()
    hadoopConf.clear()
    Seq("core-site.xml", "hdfs-site.xml").foreach { name =>
      val file = new File(configPath, name)
      if (file.isFile) {
        hadoopConf.addResource(new Path(file.toURI))
      }
    }
    loginKerberosIfEnabled(
      configPath,
      hadoopConf.get(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY))
    val hdfsCluster = new MiniDFSCluster.Builder(hadoopConf)
      .numDataNodes(config.dnCount)
      .format(true)
      .waitSafeMode(true)
      .build()

    hdfsCluster.waitActive()

    val merged = hdfsCluster.getConfiguration(0)
    // Re-apply original core-site settings that MiniDFSCluster may not propagate to merged config.
    hadoopConf.iterator().asScala.foreach { entry =>
      if (merged.get(entry.getKey) == null) {
        merged.set(entry.getKey, entry.getValue)
      }
    }
    // Always preserve the static group mapping override so HDFS NameNode
    // doesn't fail resolving virtual Kerberos principals (yarn, hdfs, etc.)
    // that don't exist as OS users.
    Option(hadoopConf.get("hadoop.user.group.static.mapping.overrides"))
      .foreach(merged.set("hadoop.user.group.static.mapping.overrides", _))
    prepareKerberosHdfsDirs(configPath, hdfsCluster.getFileSystem())
    saveConfig(merged, new File(configPath + "/core-site.xml"))
    new File(configPath + "/hdfs.ready").createNewFile()
  }

  private def prepareKerberosHdfsDirs(configPath: String, fs: org.apache.hadoop.fs.FileSystem): Unit = {
    val clusterProps = loadProperties(new File(configPath, "cluster.conf"))
    if (clusterProps.get("kerberos.enabled").forall(_.toBoolean)) {
      val openPerms = new FsPermission(0x1ff.toShort) // rwxrwxrwx (777)
      val proxyUser = clusterProps.getOrElse("livy.test.proxyUser", "proxy")
      // Directories needed by YARN (nodeattributes), spark-submit staging (/user/<name>),
      // and YARN node labels. All must be writable by virtual KDC service accounts.
      Seq("/user", "/user/yarn", "/user/livy", s"/user/$proxyUser",
          "/yarn", "/yarn/nodeattributes", "/yarn/nodelabels",
          "/tmp").foreach { dir =>
        val path = new Path(dir)
        fs.mkdirs(path)
        fs.setPermission(path, openPerms)
      }
    }
  }

}

object MiniYarnMain extends MiniClusterBase {

  override protected def start(config: MiniClusterConfig, configPath: String): Unit = {
    val baseConfig = new YarnConfiguration()
    baseConfig.clear()
    Seq("core-site.xml", "yarn-site.xml").foreach { name =>
      val file = new File(configPath, name)
      if (file.isFile) {
        baseConfig.addResource(new Path(file.toURI))
      }
    }
    loginKerberosIfEnabled(configPath, baseConfig.get(YarnConfiguration.RM_PRINCIPAL))
    baseConfig.setFloat(YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 100.0f)
    val yarnCluster = new MiniYARNCluster(getClass().getName(), config.nmCount,
      config.localDirCount, config.logDirCount)
    yarnCluster.init(baseConfig)

    // This allows applications run by YARN during the integration tests to find PIP modules
    // installed in the user's home directory (instead of just the global ones).
    baseConfig.set(YarnConfiguration.NM_USER_HOME_DIR, sys.env("HOME"))

    // Install a shutdown hook for stop the service and kill all running applications.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run(): Unit = yarnCluster.stop()
    })

    yarnCluster.start()

    // Workaround for YARN-2642.
    val yarnConfig = yarnCluster.getConfig()
    eventually(timeout(30 seconds), interval(100 millis)) {
      assert(yarnConfig.get(YarnConfiguration.RM_ADDRESS).split(":")(1) != "0",
        "RM not up yes.")
    }

    info(s"RM address in configuration is ${yarnConfig.get(YarnConfiguration.RM_ADDRESS)}")
    saveConfig(yarnConfig, new File(configPath + "/yarn-site.xml"))
    new File(configPath + "/yarn.ready").createNewFile()
  }

}

object MiniLivyMain extends MiniClusterBase {
  protected def baseLivyConf(configPath: String): Map[String, String] = {
    val baseConf = Map(
      LivyConf.LIVY_SPARK_MASTER.key -> "yarn",
      LivyConf.LIVY_SPARK_DEPLOY_MODE.key -> "cluster",
      LivyConf.HEARTBEAT_WATCHDOG_INTERVAL.key -> "1s",
      LivyConf.YARN_POLL_INTERVAL.key -> "500ms",
      LivyConf.RECOVERY_MODE.key -> "recovery",
      LivyConf.RECOVERY_STATE_STORE.key -> "filesystem",
      LivyConf.RECOVERY_STATE_STORE_URL.key -> s"file://$configPath/state-store")
    val thriftEnabled = sys.env.get("LIVY_TEST_THRIFT_ENABLED")
    if (thriftEnabled.nonEmpty && thriftEnabled.forall(_.toBoolean)) {
      baseConf + (LivyConf.THRIFT_SERVER_ENABLED.key -> "true")
    } else {
      baseConf
    }
  }

  def start(config: MiniClusterConfig, configPath: String): Unit = {
    var livyConf = baseLivyConf(configPath)
    val clusterProps = loadProperties(new File(configPath, "cluster.conf"))
    if (clusterProps.get("kerberos.enabled").forall(_.toBoolean)) {
      clusterProps.foreach { case (k, v) =>
        if (k.startsWith("livy.")) {
          livyConf += (k -> v)
        }
      }
    }

    if (Cluster.isRunningOnTravis) {
      livyConf ++= Map("livy.server.yarn.app-lookup-timeout" -> "2m")
    }

    saveProperties(livyConf, new File(configPath + "/livy.conf"))

    val server = new LivyServer()
    server.start()
    server.livyConf.set(LivyConf.ENABLE_HIVE_CONTEXT, false)
    // Write a serverUrl.conf file to the conf directory with the location of the Livy
    // server. Do it atomically since it's used by MiniCluster to detect when the Livy server
    // is up and ready.
    eventually(timeout(30 seconds), interval(1 second)) {
      var serverUrlConf = Map("livy.server.server-url" -> server.serverUrl())
      server.getJdbcUrl.foreach { url =>
        serverUrlConf += ("livy.server.thrift.jdbc-url" -> url)
      }
      saveProperties(serverUrlConf, new File(configPath + "/serverUrl.conf"))
    }
  }
}

private case class ProcessInfo(process: Process, logFile: File)

/**
 * Cluster implementation that uses HDFS / YARN mini clusters running as sub-processes locally.
 * Launching Livy through this mini cluster results in three child processes:
 *
 * - A HDFS mini cluster
 * - A YARN mini cluster
 * - The Livy server
 *
 * Each service will write its client configuration to a temporary directory managed by the
 * framework, so that applications can connect to the services.
 *
 * framework, so that applications can connect to the services.
 */
class MiniCluster(config: Map[String, String]) extends Cluster with MiniClusterUtils with Logging {
  private var _livyEndpoint: String = _
  private var _livyThriftJdbcUrl: Option[String] = None
  private var _hdfsScrathDir: Path = _

  protected val tempDir = new File(s"${sys.props("java.io.tmpdir")}/livy-int-test")
  private var _sparkConfigDir: File = _
  private var _configDir: File = _

  private var hdfs: Option[ProcessInfo] = None
  private var yarn: Option[ProcessInfo] = None
  private var livy: Option[ProcessInfo] = None

  protected var clusterConfig: Map[String, String] = config
  protected var extraJvmArgs: Seq[String] = Nil

  protected var _authScheme: String = _
  protected var _clusterUser: String = "livy"
  private var _password: String = _
  private var _sslCertPath: String = _

  protected var _principal: String = _
  protected var _keytabPath: String = _

  override def configDir(): File = _configDir

  override def hdfsScratchDir(): Path = _hdfsScrathDir

  override def doAsClusterUser[T](task: => T): T = task

  // Explicitly remove the "test-lib" dependency from the classpath of child processes. We
  // want tests to explicitly upload this jar when necessary, to test those code paths.
  protected def extraClasspathJars: Seq[String] = Nil

  private def isEmbeddedServiceJar(fileName: String): Boolean = {
    val name = fileName.toLowerCase
    name.startsWith("hive-") ||
      name.startsWith("hbase-") ||
      name.startsWith("kafka_") ||
      name.contains("ozone-") ||
      name.startsWith("curator-test-")
  }

  private def filterChildClasspath(
      excludeEmbeddedServiceJars: Boolean): String = {
    val cp = sys.props("java.class.path").split(File.pathSeparator)
    val filtered = cp.filter { path =>
      val name = new File(path).getName()
      !name.startsWith("livy-test-lib-") &&
        (!excludeEmbeddedServiceJars || !isEmbeddedServiceJar(name))
    }
    assert(cp.size != filtered.size, "livy-test-lib jar not found in classpath!")
    filtered.distinct.mkString(File.pathSeparator)
  }

  private def hadoopChildClasspath: String = filterChildClasspath(excludeEmbeddedServiceJars = true)

  private def livyChildClasspath: String = hadoopChildClasspath

  private def extraJavaTestArgs: Seq[String] = {
    Option(System.getProperty("extraJavaTestArgs"))
      .map(_.split("\\s+").toSeq).getOrElse(Nil)
  }

  /** Hook for test-scoped Kerberos setup before HDFS/YARN subprocesses start. */
  protected def configureClusterConfig(
      configDir: File,
      initialConfig: Map[String, String]): Map[String, String] = {
    _authScheme = initialConfig.getOrElse("authScheme", "")
    _clusterUser = initialConfig.getOrElse("user", "livy")
    _password = initialConfig.getOrElse("password", "")
    _sslCertPath = initialConfig.getOrElse("sslCertPath", "")
    _principal = initialConfig.getOrElse("principal", "")
    _keytabPath = initialConfig.getOrElse("keytabPath", "")
    initialConfig
  }

  protected def afterCleanup(): Unit = {}

  override def deploy(): Unit = {
    if (tempDir.exists()) {
      FileUtils.deleteQuietly(tempDir)
    }
    assert(tempDir.mkdir(), "Cannot create temp test dir.")
    _sparkConfigDir = mkdir("spark-conf")

    val sparkConf = {
      val base = Map(
        "spark.executor.instances" -> "1",
        "spark.scheduler.minRegisteredResourcesRatio" -> "0.0",
        "spark.ui.enabled" -> "false",
        SparkLauncher.DRIVER_MEMORY -> "512m",
        SparkLauncher.EXECUTOR_MEMORY -> "512m",
        SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS -> "-Dtest.appender=console",
        SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS -> "-Dtest.appender=console")
      val ozoneCp = extraClasspathJars.mkString(File.pathSeparator)
      if (ozoneCp.nonEmpty) {
        base ++ Map(
          "spark.driver.extraClassPath" -> ozoneCp,
          "spark.executor.extraClassPath" -> ozoneCp)
      } else {
        base
      }
    }
    saveProperties(sparkConf, new File(_sparkConfigDir, "spark-defaults.conf"))

    _configDir = mkdir("hadoop-conf")
    clusterConfig = configureClusterConfig(_configDir, clusterConfig)
    saveProperties(clusterConfig, new File(configDir, "cluster.conf"))
    hdfs = Some(start(MiniHdfsMain.getClass, new File(configDir, "hdfs.ready"),
      extraJavaTestArgs ++ extraJvmArgs, hadoopChildClasspath))
    yarn = Some(start(MiniYarnMain.getClass, new File(configDir, "yarn.ready"),
      extraJavaTestArgs ++ extraJvmArgs, hadoopChildClasspath))
    runLivy()

    _hdfsScrathDir = fs.makeQualified(new Path("/"))
  }

  override def cleanUp(): Unit = {
    Seq(hdfs, yarn, livy).flatten.foreach(stop)
    hdfs = None
    yarn = None
    livy = None
    afterCleanup()
  }

  def runLivy(): Unit = {
    assert(!livy.isDefined)
    val confFile = new File(configDir, "serverUrl.conf")
    val jacocoArgs = Option(TestUtils.getJacocoArgs())
      .map { args =>
        Seq(args, s"-Djacoco.args=$args")
      }.getOrElse(Nil)
    val localLivy = start(MiniLivyMain.getClass, confFile,
      jacocoArgs ++ extraJavaTestArgs ++ extraJvmArgs, livyChildClasspath)

    val props = loadProperties(confFile)
    _livyEndpoint = config.getOrElse("livyEndpoint", props("livy.server.server-url"))
    _livyThriftJdbcUrl = props.get("livy.server.thrift.jdbc-url")

    // Wait until Livy server responds.
    val httpClient = new DefaultAsyncHttpClient()
    eventually(timeout(30 seconds), interval(1 second)) {
      val res = httpClient.prepareGet(_livyEndpoint + "/metrics").execute().get()
      assert(res.getStatusCode() == HttpServletResponse.SC_OK)
    }

    livy = Some(localLivy)
  }

  def stopLivy(): Unit = {
    assert(livy.isDefined)
    livy.foreach(stop)
    _livyEndpoint = null
    _livyThriftJdbcUrl = None
    livy = None
  }

  def livyEndpoint: String = _livyEndpoint
  def jdbcEndpoint: Option[String] = _livyThriftJdbcUrl

  override def authScheme: String = _authScheme
  override def user: String = _clusterUser
  override def password: String = _password
  override def sslCertPath: String = _sslCertPath

  override def principal: String = _principal
  override def keytabPath: String = _keytabPath

  private def mkdir(name: String, parent: File = tempDir): File = {
    val dir = new File(parent, name)
    if (!dir.exists()) {
      assert(dir.mkdir(), s"Failed to create directory $name.")
    }
    dir
  }

  private def start(
      klass: Class[_],
      configFile: File,
      extraJavaArgs: Seq[String] = Nil,
      classpath: String = livyChildClasspath): ProcessInfo = {
    val simpleName = klass.getSimpleName().stripSuffix("$")
    val procDir = mkdir(simpleName)
    val procTmp = mkdir("tmp", parent = procDir)

    // Before starting anything, clean up previous running sessions.
    sys.process.Process(s"pkill -f $simpleName") !

    val cmd =
      Seq(
        sys.props("java.home") + "/bin/java",
        "-Dtest.appender=console",
        "-Djava.io.tmpdir=" + procTmp.getAbsolutePath(),
        "-cp", classpath + File.pathSeparator + configDir.getAbsolutePath()) ++
      extraJavaArgs ++
      Seq(
        klass.getName().stripSuffix("$"),
        configDir.getAbsolutePath())

    val logFile = new File(procDir, "output.log")
    val pb = new ProcessBuilder(cmd.toArray: _*)
      .directory(procDir)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))

    pb.environment().put("LIVY_CONF_DIR", configDir.getAbsolutePath())
    pb.environment().put("HADOOP_CONF_DIR", configDir.getAbsolutePath())
    pb.environment().put("SPARK_CONF_DIR", _sparkConfigDir.getAbsolutePath())
    pb.environment().put("SPARK_LOCAL_IP", "127.0.0.1")
    // kinit (native) reads KRB5_CONFIG.
    // Java GSSAPI reads java.security.krb5.conf system property.
    // JAVA_TOOL_OPTIONS propagates JVM options to all child JVMs (spark-submit, etc.).
    clusterConfig.get("krb5ConfPath").foreach { krb5Conf =>
      pb.environment().put("KRB5_CONFIG", krb5Conf)
      val existingJavaOpts = Option(pb.environment().get("JAVA_TOOL_OPTIONS")).getOrElse("")
      val krb5Opt = s"-Djava.security.krb5.conf=$krb5Conf"
      if (!existingJavaOpts.contains("krb5.conf")) {
        pb.environment().put("JAVA_TOOL_OPTIONS",
          (existingJavaOpts + " " + krb5Opt).trim)
      }
    }

    val child = pb.start()

    // Wait for the config file to show up before returning, so that dependent services
    // can see the configuration. Exit early if process dies.
    // Use a longer timeout for services that may require Kerberos initialization.
    val startupTimeout = if (simpleName == "MiniLivyMain") 90.seconds else 30.seconds
    try {
      eventually(timeout(startupTimeout), interval(100 millis)) {
        assert(configFile.isFile(), s"$simpleName hasn't started yet.")

        try {
          val exitCode = child.exitValue()
          throw new IOException(s"Child process exited unexpectedly (exit code $exitCode)")
        } catch {
          case _: IllegalThreadStateException => // Try again.
        }
      }
    } catch {
      case t: Throwable =>
        val logContent = if (logFile.isFile) {
          try {
            val src = scala.io.Source.fromFile(logFile)
            try src.mkString finally src.close()
          } catch { case _: Exception => "<unreadable>" }
        } else {
          "<not created>"
        }
        error(s"$simpleName failed to start. Process log (${logFile.getAbsolutePath}):\n$logContent")
        child.destroy()
        throw t
    }

    ProcessInfo(child, logFile)
  }

  private def stop(svc: ProcessInfo): Unit = {
    svc.process.destroy()
    svc.process.waitFor()
  }
}
