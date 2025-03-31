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
    FailureIngestionFileSystem.failPreCopyFromLocalFileNameRegex = Seq.empty
    FailureIngestionFileSystem.failureCreateAtomicRegex = Seq.empty
    FailureIngestionFileSystem.shouldFailExist = false
  }

  implicit def toArray(str: String): Array[Byte] = if (str != null) str.getBytes else null

  test("Basic RocksDB SST File Upload Failure Handling") {
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
          hadoopConf = hadoopConf
        ) { db =>
          db.put("version", "1.1")
          db.commit()

          FailureIngestionFileSystem.failPreCopyFromLocalFileNameRegex = Seq(".*sst")
          db.put("version", "2.1")
          intercept[IOException] {
            db.commit()
          }

          db.load(1)

          FailureIngestionFileSystem.failPreCopyFromLocalFileNameRegex = Seq.empty
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

  test("Basic RocksDB Zip File Upload Failure Handling") {
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
          hadoopConf = hadoopConf
        ) { db =>
          db.put("version", "1.1")
          db.commit()

          db.load(1)
          FailureIngestionFileSystem.failureCreateAtomicRegex = Seq(".*zip")
          db.put("version", "2.1")
          intercept[IOException] {
            db.commit()
          }

          db.load(1)

          FailureIngestionFileSystem.failureCreateAtomicRegex = Seq.empty
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

  // This test is to simulate the case where a previous task had connectivity problem that couldn't
  // be killed or write zip file. Only after the later one is successfully committed, it come back
  // and write the zip file.
  // The final validation isn't necessary for V1 but we just would like to make sure
  // FailureIngestionCheckpointFileManager has correct behavior --  allows zip files to be delayed
  // to be written, so that the test for V1 is valid.
  test("Zip File Overwritten by Previous Task Checkpoint V1") {
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
          hadoopConf = hadoopConf
        ) { db =>
          db.put("version", "1.1")
          db.commit()

          db.load(1)
          FailureIngestionFileSystem.createAtomicDelayCloseRegex = Seq(".*zip")
          db.put("version", "2.1")
          intercept[IOException] {
            db.commit()
          }

          FailureIngestionFileSystem.createAtomicDelayCloseRegex = Seq.empty

          db.load(1)

          db.put("version", "2.2")
          db.commit()

          assert(FailureIngestionFileSystem.delayedStreams.size == 1)
          FailureIngestionFileSystem.delayedStreams.foreach(_.close())
        }
        withDB(
          remoteDir.getAbsolutePath,
          version = 2,
          conf = conf,
          hadoopConf = hadoopConf
        ) { db =>
          // Assuming previous 2.zip overwrites, we should see the previous value.
          assert(new String(db.get("version"), "UTF-8") == "2.1")
        }
      }
    }
   }

  // This test is to simulate the case where a previous task had connectivity problem that couldn't
  // be killed or write zip file. Only after the later one is successfully committed, it come back
  // and write the zip file.
  test("Zip File Overwritten by Previous Task Checkpoint V2") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureIngestionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "false") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        var checkpointId2: Option[String] = None
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true
        ) { db =>
          db.put("version", "1.1")
          db.commit()

          val checkpointId1 = getCheckpointId(db)

          FailureIngestionFileSystem.createAtomicDelayCloseRegex = Seq(".*zip")
          db.load(1, checkpointId1)
          db.put("version", "2.1")
          intercept[IOException] {
            db.commit()
          }

          FailureIngestionFileSystem.createAtomicDelayCloseRegex = Seq.empty

          db.load(1, checkpointId1)

          db.put("version", "2.2")
          db.commit()

          checkpointId2 = getCheckpointId(db)

          // assert(FailureIngestionFileSystem.delayedStreams.size == 1)
          FailureIngestionFileSystem.delayedStreams.foreach(_.close())
        }
        withDB(
          remoteDir.getAbsolutePath,
          version = 2,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true,
          checkpointId = checkpointId2
        ) { db =>
          assert(new String(db.get("version"), "UTF-8") == "2.2")
        }
      }
    }
  }

  // This test is to simulate the case where a previous task had connectivity problem that couldn't
  // be killed or write changelog file. Only after the later one is successfully committed, it come
  // back and write the changelog file.
  // In the end, the test validates that the state store has the value of the old overwriting
  // changelog. This change is not necessary but we would like to be here to ensure that the failure
  // ingestion codee works properly so that the v2 test result is valid.
  test("Changelog File Overwritten by Previous Task With Changelog Checkpoint V1") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureIngestionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "5") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        logWarning(s"$conf")
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = false
        ) { db =>
          db.put("version", "1.1")
          db.commit()

          FailureIngestionFileSystem.createAtomicDelayCloseRegex = Seq(".*/2\\.changelog")

          db.load(1)
          db.put("version", "2.1")
          intercept[IOException] {
            db.commit()
          }

          FailureIngestionFileSystem.createAtomicDelayCloseRegex = Seq.empty

          db.load(1)

          db.put("version", "2.2")
          db.commit()

          // assert(FailureIngestionFileSystem.delayedStreams.size == 1)
          FailureIngestionFileSystem.delayedStreams.foreach(_.close())

        }
        withDB(
          remoteDir.getAbsolutePath,
          version = 2,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = false
        ) { db =>

          assert(new String(db.get("version"), "UTF-8") == "2.1")
        }
      }
    }
  }

  // This test is to simulate the case where a previous task had connectivity problem that couldn't
  // be killed or write changelog file. Only after the later one is successfully committed, it come
  // back and write the changelog file.
  test("Changelog File Overwritten by Previous Task With Changelog Checkpoint V2") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureIngestionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "5") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))

        var checkpointId2: Option[String] = None
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true
        ) { db =>
          db.put("version", "1.1")
          db.commit()

          val checkpointId1 = getCheckpointId(db)

          FailureIngestionFileSystem.createAtomicDelayCloseRegex = Seq(".*/2_.*changelog")

          db.load(1, checkpointId1)
          db.put("version", "2.1")
          intercept[IOException] {
            db.commit()
          }

          FailureIngestionFileSystem.createAtomicDelayCloseRegex = Seq.empty

          db.load(1, checkpointId1)

          db.put("version", "2.2")
          db.commit()

          checkpointId2 = getCheckpointId(db)

          // assert(FailureIngestionFileSystem.delayedStreams.size == 1)
          FailureIngestionFileSystem.delayedStreams.foreach(_.close())

          db.load(1, checkpointId1)
          db.load(2, checkpointId2)
        }

        withDB(
          remoteDir.getAbsolutePath,
          version = 2,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true,
          checkpointId = checkpointId2
        ) { db =>
          assert(new String(db.get("version"), "UTF-8") == "2.2")
        }
      }
    }
  }

  // This test is to simulate the case a delayed async snapshot from a finally non-committed version
  // succeeds later. In checkpoint V2, this snapshot shouldn't take effective. Otherwie, it will
  // break the strong consisstency condidtion guaranteed by V2.
  test("Delay Snapshot V2") {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "FailureIngestionCheckpointFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    withTempDir { remoteDir =>
      withSQLConf(
        RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "2") {
        val conf = RocksDBConf(StateStoreConf(SQLConf.get))
        var checkpointId3: Option[String] = None
        withDB(
          remoteDir.getAbsolutePath,
          version = 0,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true
        ) { db =>
          db.put("version", "1.1")
          db.commit()

          val checkpointId1 = getCheckpointId(db)

          withDB(
            remoteDir.getAbsolutePath,
            version = 1,
            conf = conf,
            hadoopConf = hadoopConf,
            enableStateStoreCheckpointIds = true,
            checkpointId = checkpointId1
          ) { db2 =>
            db2.put("version", "2.1")
            db2.commit()

            db.load(1, checkpointId1)
            db.put("version", "2.2")
            db.commit()
            val checkpointId2 = getCheckpointId(db)

            db.load(2, checkpointId2)
            db.put("foo", "bar")
            db.commit()

            db2.doMaintenance()
          }

          checkpointId3 = getCheckpointId(db)
        }
        withDB(
          remoteDir.getAbsolutePath,
          version = 3,
          conf = conf,
          hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = true,
          checkpointId = checkpointId3
        ) { db =>
          assert(new String(db.get("version"), "UTF-8") == "2.2")
          assert(new String(db.get("foo"), "UTF-8") == "bar")
        }
      }
    }
  }

  def getCheckpointId(db: RocksDB): Option[String] = {
    val ci = db.getLatestCheckpointInfo(0)
    ci.stateStoreCkptId
  }

  def withDB[T](
      remoteDir: String,
      version: Int,
      conf: RocksDBConf,
      hadoopConf: Configuration = new Configuration(),
      enableStateStoreCheckpointIds: Boolean = false,
      checkpointId: Option[String] = None)(
      func: RocksDB => T): T = {
    var db: RocksDB = null
    try {
      db = FailureIngestionRocksDBStateStoreProvider.createRocksDBWithFaultIngestion(
        remoteDir,
        conf = conf,
        localRootDir = Utils.createTempDir(),
        hadoopConf = hadoopConf,
        loggingId = s"[Thread-${Thread.currentThread.getId}]",
        useColumnFamilies = false,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds)
      db.load(version, checkpointId)
      func(db)
    } finally {
      if (db != null) {
        db.close()
      }
    }
  }
}
