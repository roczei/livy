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

import java.net.URI

import org.apache.livy.LivyConf
import org.apache.livy.sessions.Session

/**
 * Collects Hadoop filesystem endpoint URIs from session inputs (jars, files, archives,
 * spark conf paths, etc.) so delegation tokens can be obtained automatically.
 */
object SessionFilesystemUriCollector {

  private val URI_CONF_KEYS = Set(
    "hive.metastore.warehouse.dir",
    "spark.hadoop.hive.metastore.warehouse.dir",
    "spark.sql.warehouse.dir",
    "ozone.om.service.ids",
    "spark.hadoop.ozone.om.service.ids")

  private val URI_PATTERN =
    """(?:hdfs|viewfs|webhdfs|swebhdfs|s3a?|wasbs?|abfs?s?|gs|gcs|ofs|o3fs)://[^\s,]+""".r

  /**
   * Scans user-provided session resources and spark configuration for remote filesystem URIs.
   * Relative paths are resolved against fs.defaultFS before extracting the filesystem endpoint.
   */
  def collect(
      livyConf: LivyConf,
      sparkConf: Map[String, String],
      jars: Seq[String] = Nil,
      files: Seq[String] = Nil,
      archives: Seq[String] = Nil,
      pyFiles: Seq[String] = Nil,
      mainFile: Option[String] = None): Set[String] = {
    if (!livyConf.getBoolean(LivyConf.DELEGATION_TOKEN_AUTO_DISCOVER_FILESYSTEMS)) {
      return Set.empty
    }

    val rawPaths = scala.collection.mutable.ArrayBuffer[String]()
    rawPaths ++= jars ++ files ++ archives ++ pyFiles
    mainFile.foreach(rawPaths += _)

    livyConf.sparkFileLists.foreach { key =>
      sparkConf.get(key).foreach { value =>
        rawPaths ++= splitPathList(value)
      }
    }

    URI_CONF_KEYS.foreach { key =>
      sparkConf.get(key).foreach(rawPaths += _)
    }

    sparkConf.foreach { case (key, value) =>
      if (key.startsWith("spark.hadoop.")) {
        rawPaths ++= extractUriLikeTokens(value)
      }
    }

    val resolved = Session.resolveURIs(rawPaths.distinct.filter(_.nonEmpty), livyConf)
    resolved.flatMap(toFilesystemEndpoint).toSet
  }

  private def splitPathList(value: String): Seq[String] = {
    value.split("[, ]+").filter(_.nonEmpty)
  }

  private def extractUriLikeTokens(value: String): Seq[String] = {
    val explicitUris = URI_PATTERN.findAllIn(value).toSeq
    val relativePaths = splitPathList(value).filter(_.startsWith("/"))
    explicitUris ++ relativePaths
  }

  private[utils] def toFilesystemEndpoint(uriStr: String): Option[String] = {
    val uri = new URI(uriStr)
    val scheme = Option(uri.getScheme).map(_.toLowerCase).getOrElse("")
    scheme match {
      case "file" | "" => None
      case _ =>
        Option(uri.getAuthority).filter(_.nonEmpty).map { authority =>
          s"${uri.getScheme}://$authority"
        }
    }
  }
}
