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
import java.net.URI

import scala.language.implicitConversions
import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.{CancellableFSDataOutputStream, RenameBasedFSDataOutputStream}
import org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

class FailureIngestionCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends FileSystemBasedCheckpointFileManager(path, hadoopConf) {

  override def createAtomic(path: Path,
                            overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    FailureIngestionFileSystem.failureCreateAtomicRegex.foreach { regex =>
      if (regex.findFirstIn(path.toString).isDefined) {
        throw new IOException("Fake File System Create Atomic Failure")
      }
    }
    new RenameBasedFSDataOutputStream(this, path, overwriteIfPossible)
  }

  override def renameTempFile(srcPath: Path, dstPath: Path,
                              overwriteIfPossible: Boolean): Unit = {
    if (!fs.exists(dstPath)) {
      // only write if a file does not exist at this location
      super.renameTempFile(srcPath, dstPath, overwriteIfPossible)
    }
  }

  override def list(path: Path, filter: PathFilter): Array[FileStatus] = {
    if (FailureIngestionFileSystem.shouldFailList) {
      throw new IOException("Fake File System List Status Failure")
    }
    super.list(path, filter)
  }

  override def exists(path: Path): Boolean = {
    if (FailureIngestionFileSystem.shouldFailExist) {
      throw new IOException("Fake File System Exist Failure")
    }
    super.exists(path)
  }
}

object FailureIngestionFileSystem {
  var shouldFailCopyFromLocalFile = false
  //var failCopyFromLocalFileNameRegex: Seq[String]
  var shouldFailList = false
  var shouldFailExist = false
  var failureCreateAtomicRegex: Option[Regex] = None
}

class FailureIngestionFileSystem(innerFs: FileSystem) extends FileSystem {

  override def getConf: Configuration = innerFs.getConf

  override def mkdirs(f: Path, permission: FsPermission): Boolean = innerFs.mkdirs(f, permission)

  override def rename(src: Path, dst: Path): Boolean = innerFs.rename(src, dst)

  override def getUri: URI = innerFs.getUri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = innerFs.open(f, bufferSize)

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    innerFs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress)

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    innerFs.append(f, bufferSize, progress)

  override def delete(f: Path, recursive: Boolean): Boolean = innerFs.delete(f, recursive)

  override def listStatus(f: Path): Array[FileStatus] = {
    if (FailureIngestionFileSystem.shouldFailList) {
      throw new IOException("Fake File System List Status Failure")
    }
    innerFs.listStatus(f)
  }

  override def setWorkingDirectory(new_dir: Path): Unit = innerFs.setWorkingDirectory(new_dir)

  override def getWorkingDirectory: Path = innerFs.getWorkingDirectory

  override def getFileStatus(f: Path): FileStatus = innerFs.getFileStatus(f)

  override def copyFromLocalFile(src: Path, dst: Path): Unit = {
    innerFs.copyFromLocalFile(src, dst)
    if (FailureIngestionFileSystem.shouldFailCopyFromLocalFile) {
      throw new IOException("Fake File System Copy Failure")
    }
  }
}

class FailureIngestionRocksDBStateStoreProvider extends RocksDBStateStoreProvider {
  override def CreateRocksDB(
      dfsRootDir: String,
      conf: RocksDBConf,
      localRootDir: File,
      hadoopConf: Configuration,
      loggingId: String,
      useColumnFamilies: Boolean,
      enableStateStoreCheckpointIds: Boolean): RocksDB = {
    FailureIngestionRocksDBStateStoreProvider.createRocksDBWithFaultIngestion(
      dfsRootDir,
      conf,
      localRootDir,
      hadoopConf,
      loggingId,
      useColumnFamilies,
      enableStateStoreCheckpointIds
    )
  }
}

object FailureIngestionRocksDBStateStoreProvider {
  def createRocksDBWithFaultIngestion(
      dfsRootDir: String,
      conf: RocksDBConf,
      localRootDir: File,
      hadoopConf: Configuration,
      loggingId: String,
      useColumnFamilies: Boolean,
      enableStateStoreCheckpointIds: Boolean): RocksDB = {
    new RocksDB(
      dfsRootDir,
      conf = conf,
      localRootDir = localRootDir,
      hadoopConf = hadoopConf,
      loggingId = loggingId,
      useColumnFamilies = useColumnFamilies,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds
    ) {
      override def CreateFileManager(
          dfsRootDir: String,
          localTempDir: File,
          hadoopConf: Configuration,
          codecName: String,
          loggingId: String): RocksDBFileManager = {
        new RocksDBFileManager(
          dfsRootDir,
          localTempDir,
          hadoopConf,
          codecName,
          loggingId = loggingId
        ) {
          override def GetFileSystem(
              myDfsRootDir: String,
              myHadoopConf: Configuration): FileSystem = {
            new FailureIngestionFileSystem(new Path(myDfsRootDir).getFileSystem(myHadoopConf))
          }
        }
      }
    }
  }
}

class RocksDBProviderCheckpointFailureIngestionSuite extends StreamTest {
  import testImplicits._

  override def beforeEach(): Unit = {
    super.beforeEach()
    FailureIngestionFileSystem.shouldFailCopyFromLocalFile = false
    FailureIngestionFileSystem.shouldFailList = false
    FailureIngestionFileSystem.shouldFailExist = false
    FailureIngestionFileSystem.failureCreateAtomicRegex = None
  }

  test("RocksDBProvider with basic failure ingestion") {
    withTempDir { dir =>
      val input = MemoryStream[Int]
      val conf = Map(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[FailureIngestionRocksDBStateStoreProvider].getName
      )

      testStream(input.toDF().groupBy().count(), outputMode = OutputMode.Update)(
        StartStream(checkpointLocation = dir.getAbsolutePath, additionalConfs = conf),
        AddData(input, 1, 2, 3),
        CheckAnswer(3),
        new ExternalAction() {
          override def runAction(): Unit = {
            FailureIngestionFileSystem.shouldFailCopyFromLocalFile = true
          }
        },
        AddData(input, 1, 2, 3),
        ExpectFailure[SparkException](),
        new ExternalAction() {
          override def runAction(): Unit = {
            FailureIngestionFileSystem.shouldFailCopyFromLocalFile = false
          }
        },
        StartStream(checkpointLocation = dir.getAbsolutePath, additionalConfs = conf),
        CheckAnswer(3, 6)
      )
    }
  }

  test("RocksDBProvider with basic maintenance failure ingestion") {
    withTempDir { dir =>
      val input = MemoryStream[Int]

      withSQLConf(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX +
        ".changelogCheckpointing.enabled" -> "true",
        SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key -> "1",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
          classOf[FailureIngestionRocksDBStateStoreProvider].getName) {
        FailureIngestionFileSystem.failureCreateAtomicRegex = Some(".*\\.zip".r)
        FailureIngestionFileSystem.shouldFailCopyFromLocalFile = true
        val conf = Map(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
            classOf[FailureIngestionRocksDBStateStoreProvider].getName
        )

        testStream(input.toDF().groupBy().count(), outputMode = OutputMode.Update)(
          StartStream(checkpointLocation = dir.getAbsolutePath, additionalConfs = conf),
          AddData(input, 1, 2, 3),
          CheckAnswer(3),
          AddData(input, 4),
          CheckAnswer(3, 4)
        )
        FailureIngestionFileSystem.failureCreateAtomicRegex = None
      }
    }
  }
}

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
