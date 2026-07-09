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

class SessionFilesystemUriCollectorSpec extends FunSpec {
  private def confWithDefaultFs(defaultFs: String): LivyConf = {
    val conf = new LivyConf()
    conf.hadoopConf.set("fs.defaultFS", defaultFs)
    conf
  }

  describe("SessionFilesystemUriCollector") {
    it("should extract filesystem endpoints from explicit session paths") {
      val livyConf = confWithDefaultFs("hdfs://nn1:8020")
      val uris = SessionFilesystemUriCollector.collect(
        livyConf,
        sparkConf = Map.empty,
        jars = Seq("hdfs://nn2:8020/user/jars/app.jar"),
        files = Seq("/data/input.csv"),
        mainFile = Some("s3a://my-bucket/scripts/job.py"))

      assert(uris === Set(
        "hdfs://nn2:8020",
        "hdfs://nn1:8020",
        "s3a://my-bucket"))
    }

    it("should scan spark file list configs and warehouse paths") {
      val livyConf = confWithDefaultFs("hdfs://nn1:8020")
      val sparkConf = Map(
        LivyConf.SPARK_ARCHIVES -> "viewfs://cluster/user/archives.zip",
        "spark.sql.warehouse.dir" -> "/warehouse",
        "spark.hadoop.hive.metastore.warehouse.dir" -> "hdfs://nn3:8020/hive/warehouse")

      val uris = SessionFilesystemUriCollector.collect(livyConf, sparkConf)
      assert(uris === Set(
        "viewfs://cluster",
        "hdfs://nn1:8020",
        "hdfs://nn3:8020"))
    }

    it("should discover URIs embedded in spark.hadoop configuration values") {
      val livyConf = confWithDefaultFs("hdfs://nn1:8020")
      val sparkConf = Map(
        "spark.hadoop.fs.s3a.bucket" -> "my-bucket",
        "spark.hadoop.fs.defaultFS" -> "abfs://storage.dfs.core.windows.net")

      val uris = SessionFilesystemUriCollector.collect(livyConf, sparkConf)
      assert(uris === Set("abfs://storage.dfs.core.windows.net"))
    }

    it("should skip local file paths") {
      val livyConf = confWithDefaultFs("hdfs://nn1:8020")
      val uris = SessionFilesystemUriCollector.collect(
        livyConf,
        sparkConf = Map.empty,
        files = Seq("file:///tmp/local.txt"))

      assert(uris.isEmpty)
    }

    it("should return empty when auto-discovery is disabled") {
      val livyConf = confWithDefaultFs("hdfs://nn1:8020")
        .set(LivyConf.DELEGATION_TOKEN_AUTO_DISCOVER_FILESYSTEMS, false)
      val uris = SessionFilesystemUriCollector.collect(
        livyConf,
        sparkConf = Map.empty,
        jars = Seq("hdfs://nn2:8020/user/jars/app.jar"))

      assert(uris.isEmpty)
    }

    it("should normalize filesystem endpoints") {
      assert(SessionFilesystemUriCollector.toFilesystemEndpoint(
        "hdfs://nn1:8020/user/data/file.parquet") === Some("hdfs://nn1:8020"))
      assert(SessionFilesystemUriCollector.toFilesystemEndpoint(
        "ofs://omservice/volume1/bucket1/key") === Some("ofs://omservice"))
      assert(SessionFilesystemUriCollector.toFilesystemEndpoint(
        "o3fs://bucket.volume.omservice/key") === Some("o3fs://bucket.volume.omservice"))
      assert(SessionFilesystemUriCollector.toFilesystemEndpoint(
        "file:///tmp/x") === None)
    }

    it("should discover Ozone OFS paths from session resources") {
      val livyConf = confWithDefaultFs("hdfs://nn1:8020")
      val uris = SessionFilesystemUriCollector.collect(
        livyConf,
        sparkConf = Map.empty,
        files = Seq("ofs://omservice/volume1/bucket1/input.csv"),
        jars = Seq("o3fs://bucket.volume.omservice/lib.jar"))

      assert(uris === Set("ofs://omservice", "o3fs://bucket.volume.omservice"))
    }
  }
}
