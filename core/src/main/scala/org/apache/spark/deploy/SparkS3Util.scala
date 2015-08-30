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

package org.apache.spark.deploy

import java.io.IOException
import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.amazonaws.{AmazonClientException, Protocol, ClientConfiguration}
import com.amazonaws.auth.{InstanceProfileCredentialsProvider, BasicAWSCredentials, STSAssumeRoleSessionCredentialsProvider, AWSCredentialsProvider}
import com.amazonaws.internal.StaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing, S3ObjectSummary}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path, RemoteIterator}
import org.apache.hadoop.fs.s3.S3Credentials
import org.apache.hadoop.io.compress.{SplittableCompressionCodec, CompressionCodecFactory}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, JobConf}

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

import com.google.common.base.{Preconditions, Strings}
import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.collect.AbstractSequentialIterator

/**
 * :: DeveloperApi ::
 * Contains util methods to interact with S3 from Spark.
 */
@DeveloperApi
object SparkS3Util extends Logging {
  private val sparkConf = new SparkConf()
  val conf: Configuration = SparkHadoopUtil.get.newConfiguration(sparkConf)

  private val s3ClientCache: Cache[String, AmazonS3Client] = CacheBuilder
    .newBuilder
    .concurrencyLevel(Runtime.getRuntime.availableProcessors)
    .build[String, AmazonS3Client]

  private val S3_CONNECT_TIMEOUT: String = "spark.s3.connect.timeout"
  private val S3_MAX_CONNECTIONS: String = "spark.s3.max.connections"
  private val S3_MAX_ERROR_RETRIES: String = "spark.s3.max.error.retries"
  private val S3_SOCKET_TIMEOUT: String = "spark.s3.socket.timeout"
  private val S3_SSL_ENABLED: String = "spark.s3.ssl.enabled"
  private val S3_USE_INSTANCE_CREDENTIALS: String = "spark.s3.use.instance.credentials"

  private val FILEINPUTFORMAT_INPUTDIR: String =
    "mapreduce.input.fileinputformat.inputdir"
  private val FILEINPUTFORMAT_NUMINPUTFILES: String =
    "mapreduce.input.fileinputformat.numinputfiles"
  private val FILEINPUTFORMAT_SPLIT_MINSIZE: String =
    "mapreduce.input.fileinputformat.split.minsize"

  private def getS3Client(bucket: String): AmazonS3Client = {
    val sslEnabled: Boolean = conf.getBoolean(S3_SSL_ENABLED, true)
    val maxErrorRetries: Int = conf.getInt(S3_MAX_ERROR_RETRIES, 10)
    val connectTimeout: Int = conf.getInt(S3_CONNECT_TIMEOUT, 5000)
    val socketTimeout: Int = conf.getInt(S3_SOCKET_TIMEOUT, 5000)
    val maxConnections: Int = conf.getInt(S3_MAX_CONNECTIONS, 500)
    val useInstanceCredentials: Boolean = conf.getBoolean(S3_USE_INSTANCE_CREDENTIALS, false)

    val clientConf: ClientConfiguration = new ClientConfiguration
    clientConf.setMaxErrorRetry(maxErrorRetries)
    clientConf.setProtocol(if (sslEnabled) Protocol.HTTPS else Protocol.HTTP)
    clientConf.setConnectionTimeout(connectTimeout)
    clientConf.setSocketTimeout(socketTimeout)
    clientConf.setMaxConnections(maxConnections)

    val s3RoleArn = Option(conf.get("aws.iam.role.arn"))
    val s3RoleArnDefault = Option(conf.get("aws.iam.role.arn.default"))
    val credentialsProvider: AWSCredentialsProvider =
      s3RoleArn match {
        case Some(role) =>
          logDebug("Use user-specified IAM role: " + role)
          new STSAssumeRoleSessionCredentialsProvider(
            role, "RoleSessionName-" + Utils.random.nextInt)
        case None if useInstanceCredentials =>
          logDebug("Use IAM role associated with the instance")
          new InstanceProfileCredentialsProvider
        case _ =>
          s3RoleArnDefault match {
            case Some(role) =>
              logDebug("Use default IAM role configured in Hadoop config: " + role)
              new STSAssumeRoleSessionCredentialsProvider(
                role, "RoleSessionName-" + Utils.random.nextInt)
            case _ =>
              try {
                logDebug("Use AWS key pairs")
                val credentials: S3Credentials = new S3Credentials
                credentials.initialize(URI.create(bucket), conf)
                new StaticCredentialsProvider(
                  new BasicAWSCredentials(credentials.getAccessKey, credentials.getSecretAccessKey))
              } catch {
                case e: Exception => throw new RuntimeException("S3 credentials not configured", e)
              }
        }
      }

    val providerName: String = credentialsProvider.getClass.getSimpleName
    val s3ClientKey: String =
      if (s3RoleArn == null) providerName
      else providerName + "_" + s3RoleArn

    Option(s3ClientCache.getIfPresent(s3ClientKey)).getOrElse {
      val newClient = new AmazonS3Client(credentialsProvider, clientConf)
      s3ClientCache.put(s3ClientKey, newClient)
      newClient
    }
  }

  private def commonPrefix(prefixes: List[String]): String = {
    if (prefixes.isEmpty || prefixes.contains("")) ""
    else prefixes.head.head match {
      case ch =>
        if (prefixes.tail.forall(_.head == ch)) "" + ch + commonPrefix(prefixes.map(_.tail))
        else ""
    }
  }

  private def keyFromPath(path: Path): String = {
    Preconditions.checkArgument(path.isAbsolute, "Path is not absolute: %s", path)
    var key: String = Strings.nullToEmpty(path.toUri.getPath)
    if (key.startsWith("/")) {
      key = key.substring(1)
    }
    if (key.endsWith("/")) {
      key = key.substring(0, key.length - 1)
    }
    key
  }

  private def listPrefix(s3: AmazonS3Client, path: Path): Iterator[LocatedFileStatus] = {
    val uri: URI = path.toUri
    val key: String = keyFromPath(path)
    val request: ListObjectsRequest = new ListObjectsRequest()
      .withBucketName(uri.getAuthority)
      .withPrefix(key)

    val listings = new AbstractSequentialIterator[ObjectListing](s3.listObjects(request)) {
      protected def computeNext(previous: ObjectListing): ObjectListing = {
        if (!previous.isTruncated) {
          return null
        }
        s3.listNextBatchOfObjects(previous)
      }
    }.asScala

    val bucket: String = uri.getScheme + "://" + uri.getAuthority
    listings
      .map(listing => statusFromObjects(bucket, listing.getObjectSummaries))
      .reduceLeft(_ ++ _)
  }

  private def statusFromObjects(
      bucket: String,
      objects: util.List[S3ObjectSummary]): Iterator[LocatedFileStatus] = {
    val blockSize: Int = conf.getInt("fs.s3n.block.size", 67108864)
    val list: ArrayBuffer[LocatedFileStatus] = ArrayBuffer()
    for (obj: S3ObjectSummary <- objects.asScala) {
      if (!obj.getKey.endsWith("/")) {
        val status: FileStatus = new FileStatus(
          obj.getSize, false, 1, blockSize,
          obj.getLastModified.getTime,
          new Path(bucket + "/" + obj.getKey))
        list += createLocatedFileStatus(status)
      }
    }
    list.iterator
  }

  private def createLocatedFileStatus(status: FileStatus): LocatedFileStatus = {
    val fakeLocation: Array[BlockLocation] = Array[BlockLocation]()
    new LocatedFileStatus(status, fakeLocation)
  }

  private def listLocatedStatus(
      s3: AmazonS3Client,
      path: Path): RemoteIterator[LocatedFileStatus] = {
    new RemoteIterator[LocatedFileStatus]() {
      private final val iterator: Iterator[LocatedFileStatus] = listPrefix(s3, path)

      def hasNext: Boolean = {
        try {
          iterator.hasNext
        } catch {
          case e: AmazonClientException => throw new IOException(e)
        }
      }

      def next(): LocatedFileStatus = {
        try {
          iterator.next()
        } catch {
          case e: AmazonClientException => throw new IOException(e)
        }
      }
    }
  }

  private def listStatus(s3: AmazonS3Client, path: String): Array[FileStatus] = {
    val list: ArrayBuffer[LocatedFileStatus] = ArrayBuffer()
    val iterator: RemoteIterator[LocatedFileStatus] = listLocatedStatus(s3, new Path(path))
    while (iterator.hasNext) {
      list += iterator.next
    }
    list.toArray
  }

  private def filterFileStatus(
      unfiltered: Array[FileStatus],
      inputDirs: Array[String]): Array[FileStatus] = {
    val filtered: ArrayBuffer[FileStatus] = ArrayBuffer()
    for (fileStatus <- unfiltered; inputDir <- inputDirs) {
      if (fileStatus.getPath.toString.startsWith(inputDir)) {
        filtered += fileStatus
      }
    }
    filtered.toArray
  }

  private def isSplitable(conf: JobConf, file: Path): Boolean = {
    val compressionCodecs = new CompressionCodecFactory(conf)
    val codec = compressionCodecs.getCodec(file)
    if (codec == null) {
      true
    } else {
      codec.isInstanceOf[SplittableCompressionCodec]
    }
  }

  /**
   * This is based on `FileInputFormat.getSplits` method. Two key differences are:
   *   1) Use `AmazonS3Client.listObjects` instead of `FileSystem.listStatus`.
   *   2) Bypass computing data locality hints since they'er irrelevent to S3 objects.
   */
  def getSplits(conf: JobConf, minSplits: Int): Array[InputSplit] = {
    val inputDirs: Array[String] = conf.get(FILEINPUTFORMAT_INPUTDIR).split(",")
    val files: Array[FileStatus] = inputDirs.toList
      .groupBy[String] { path =>
        val uri = URI.create(path)
        uri.getScheme + "://" + uri.getAuthority
      }
      .mapValues[String](f = commonPrefix)
      .map { case (bucket, prefix) => (bucket, listStatus(getS3Client(bucket), prefix)) }
      .mapValues[Array[FileStatus]] { arr => filterFileStatus(arr, inputDirs) }
      .values.reduceLeft(_ ++ _)
    conf.setLong(FILEINPUTFORMAT_NUMINPUTFILES, files.length)

    val totalSize: Long = files.foldLeft(0L) { (sum, file) => sum + file.getLen }
    val goalSize: Long = totalSize / (if (minSplits == 0) 1 else minSplits)
    val minSize: Long = conf.getLong(FILEINPUTFORMAT_SPLIT_MINSIZE, 1)
    val splits: ArrayBuffer[InputSplit] = ArrayBuffer[InputSplit]()
    val fakeHosts: Array[String] = Array()
    for (file <- files) {
      val path: Path = file.getPath
      val length: Long = file.getLen
      if (length > 0 && isSplitable(conf, path)) {
        val blockSize: Long = file.getBlockSize
        val splitSize: Long = Math.max(minSize, Math.min(goalSize, blockSize))
        var bytesRemaining: Long = length
        while (bytesRemaining.toDouble / splitSize > 1.1) {
          splits += new FileSplit(path, length - bytesRemaining, splitSize, fakeHosts)
          bytesRemaining -= splitSize
        }
        if (bytesRemaining != 0) {
          splits += new FileSplit(path, length - bytesRemaining, bytesRemaining, fakeHosts)
        }
      } else {
        splits += new FileSplit(path, 0, length, fakeHosts)
      }
    }

    logDebug("Total size of input splits is " + totalSize)
    logDebug("Num of input splits is " + splits.size)

    splits.toArray
  }
}
