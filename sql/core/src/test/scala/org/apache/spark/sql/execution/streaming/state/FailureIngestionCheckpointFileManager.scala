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

import scala.util.matching.Regex

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.sql.execution.streaming.CheckpointFileManager.{CancellableFSDataOutputStream, RenameBasedFSDataOutputStream}
import org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager

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
  // var failCopyFromLocalFileNameRegex: Seq[String]
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
