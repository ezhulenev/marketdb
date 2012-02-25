package com.ergodicity.marketdb.loader

import scalaz._
import Scalaz._
import scalaz.effects.IO
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import java.io._
import org.apache.commons.httpclient.{HttpStatus, HttpClient}
import org.apache.commons.httpclient.methods.GetMethod


sealed abstract class DataRef

case class InputStreamRef(is: InputStream) extends DataRef

case class LocalRef(file: File) extends DataRef

case class RemoteRef(url: String) extends DataRef


trait RemoteFetcherCache {
  def cache(ref: RemoteRef, is: InputStream): InputStream
}

/**
 * Fetch market data from Data Reference
 */
trait DataFetcher[R <: DataRef] {
  def toStream(ref: R): IO[InputStream]
}

object DataFetcher {
  val log = LoggerFactory.getLogger("DataFetcher")

  implicit def InputStreamFetcher: DataFetcher[InputStreamRef] = new DataFetcher[InputStreamRef] {
    def toStream(ref: InputStreamRef) = ref.is.pure[IO]
  }

  implicit def LocalFetcher: DataFetcher[LocalRef] = new DataFetcher[LocalRef] {
    def toStream(ref: LocalRef) = {
      ref.pure[IO] map {ref =>
          new FileInputStream(ref.file)
      }
    }
  }

  implicit def RemoteFetcher(implicit client: HttpClient, cache: RemoteFetcherCache): DataFetcher[RemoteRef] = new DataFetcher[RemoteRef] {
    def toStream(ref: RemoteRef) = {
      ref.pure[IO] map {
        ref =>
          val get = new GetMethod(ref.url)
          try {
            val code = client.executeMethod(get)
            if (code != HttpStatus.SC_OK) {
              throw new IOException("Error response code fetching RemoteRef: " + code + "; Ref = " + ref)
            }
            cache.cache(ref, get.getResponseBodyAsStream)
          } finally {
            get.releaseConnection()
          }

      }
    }
  }
}



class LocalMirrorCache(dir: File, url: String) extends RemoteFetcherCache {
  val log = LoggerFactory.getLogger(classOf[LocalMirrorCache])

  if (!dir.isDirectory) {
    throw new IllegalArgumentException("Directory doesn't exists: " + dir)
  }

  private def generateTempFileName(ref: RemoteRef): String = {
    val fileName = ref.url.split("/").last
    val now = new DateTime
    now.getMillisOfDay.toString + "_" + fileName + ".tmp"
  }

  def cache(ref: RemoteRef, is: InputStream) = {
    log.debug("Cache remote: " + ref + " to local mirror")
    import scalax.io._
    import Resource._

    val tempDir = new File(System.getProperty("java.io.tmpdir"))
    val tempFile = new File(tempDir, generateTempFileName(ref))
    log.debug("Write InputStream to temp location: " + tempFile.getAbsolutePath)

    // -- Download file to temp location
    fromInputStream(is) copyDataTo fromFile(tempFile)

    // -- Copy data from temp to mirror target
    val fileName = ref.url.split("/").last
    val filePath = ref.url.replace(url, "").split("/").dropRight(1).mkString("/")

    val targetDir = new File(dir, filePath)
    log.debug("Copy from temp location to target: " + targetDir.getAbsolutePath + "; File name: " + fileName)

    if (!targetDir.isDirectory && !targetDir.mkdirs()) {
      throw new RuntimeException("Can't create required directories for path: " + targetDir.getAbsolutePath)
    }

    val targetFile = new File(targetDir, fileName)
    fromFile(tempFile) copyDataTo fromFile(targetFile)

    new FileInputStream(targetFile)
  }
}











