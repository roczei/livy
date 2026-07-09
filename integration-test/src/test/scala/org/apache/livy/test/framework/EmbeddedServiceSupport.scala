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

import java.io.File
import java.net.ServerSocket

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/** Shared helpers for embedded Hive/HBase/Kafka test services. */
private[framework] object EmbeddedServiceSupport extends MiniClusterUtils {

  def isServiceRequested(config: Map[String, String], enabledKey: String): Boolean = {
    config.get(enabledKey).forall(_.toBoolean)
  }

  def isClassAvailable(className: String): Boolean = {
    try {
      Class.forName(className)
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  def findFreePort(): Int = {
    val socket = new ServerSocket(0)
    try {
      socket.getLocalPort
    } finally {
      socket.close()
    }
  }

  def runtimeJars(keywords: Seq[String]): Seq[String] = {
    sys.props("java.class.path").split(File.pathSeparator)
      .filter { path =>
        val name = new File(path).getName.toLowerCase
        keywords.exists(name.contains)
      }
      .distinct
  }

  def mergeIntoCoreSite(configDir: File, keys: Seq[String], source: Configuration): Unit = {
    val coreFile = new File(configDir, "core-site.xml")
    val coreConf = new Configuration(false)
    if (coreFile.isFile) {
      coreConf.addResource(new Path(coreFile.toURI))
    }
    keys.foreach { key =>
      Option(source.get(key)).foreach(coreConf.set(key, _))
    }
    saveConfig(coreConf, coreFile)
  }
}
