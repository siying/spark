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

package org.apache.spark.sql.execution.streaming.state

import java.io._

import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils


@SlowSQLTest
class RocksDBCheckpointFailureIngestionSuite extends StreamTest
  with SharedSparkSession {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.STATE_STORE_PROVIDER_CLASS, classOf[RocksDBStateStoreProvider].getName)

  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    FailureIngestionFileSystem.shouldFailCopyFromLocalFile = false
    FailureIngestionFileSystem.shouldFailList = false
    FailureIngestionFileSystem.shouldFailExist = false
  }

  implicit def toArray(str: String): Array[Byte] = if (str != null) str.getBytes else null

  test("Basic RocksDB Checkpoint File Write Failure Handling") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureIngestionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf,
          useColumnFamilies = true
        ) { db =>
          db.put("version", "1.1")
          db.commit()

            FailureIngestionFileSystem.shouldFailCopyFromLocalFile = true
          db.put("version", "2.1")
          intercept[IOException] {
            db.commit()
          }

          db.load(1)

            FailureIngestionFileSystem.shouldFailCopyFromLocalFile = false
          var ex = intercept[SparkException] {
            db.load(2)
          }
          checkError(
            ex,
            condition = "CANNOT_LOAD_STATE_STORE.CANNOT_READ_STREAMING_STATE_FILE",
            parameters = Map(
              "fileToRead" -> s"$remoteDir/2.changelog"
            )
          )

          db.load(0)
            FailureIngestionFileSystem.shouldFailExist = true
          var ex2 = intercept[IOException] {
            db.load(1)
          }
        }
      }
    }
  }

  def withDB[T](
      remoteDir: String,
      version: Int,
      conf: RocksDBConf,
      hadoopConf: Configuration = new Configuration(),
      useColumnFamilies: Boolean = false,
      localDir: File = Utils.createTempDir())(
      func: RocksDB => T): T = {
    var db: RocksDB = null
    try {
      db = FailureIngestionRocksDBStateStoreProvider.createRocksDBWithFaultIngestion(
        remoteDir,
        conf = conf,
        localRootDir = localDir,
        hadoopConf = hadoopConf,
        loggingId = s"[Thread-${Thread.currentThread.getId}]",
        useColumnFamilies = useColumnFamilies,
        enableStateStoreCheckpointIds = false)
      db.load(version)
      func(db)
    } finally {
      if (db != null) {
        db.close()
      }
    }
  }
}
