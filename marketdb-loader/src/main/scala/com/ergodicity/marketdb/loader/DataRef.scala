package com.ergodicity.marketdb.loader

import scalaz._
import Scalaz._
import scalaz.effects.IO
import java.io.{FileInputStream, File, InputStream}


sealed abstract class DataRef

case class InputStreamRef(is: InputStream) extends DataRef
case class LocalRef(file: File) extends DataRef
case class RemoteRef(url: String) extends DataRef


trait RemoteFetcherCache {
  def cache: String
}

/**
 * Fetch market data from Data Reference
 */
trait DataFetcher[R <: DataRef] {
  def toStream(ref: R): IO[InputStream]
}

object DataFetcher {
  implicit def InputStreamFetcher: DataFetcher[InputStreamRef] = new DataFetcher[InputStreamRef] {
    def toStream(ref: InputStreamRef) = ref.is.pure[IO]
  }

  implicit def LocalFetcher: DataFetcher[LocalRef] = new DataFetcher[LocalRef] {
    private def is(ref: LocalRef): InputStream = new FileInputStream(ref.file)
    def toStream(ref: LocalRef) = is(ref).pure[IO]
  }

  implicit def RemoteFetcher(implicit cache: RemoteFetcherCache): DataFetcher[RemoteRef] = new DataFetcher[RemoteRef] {
    def toStream(ref: RemoteRef) = throw new UnsupportedOperationException("Remote Fetcher Not Supported")
  }
}











