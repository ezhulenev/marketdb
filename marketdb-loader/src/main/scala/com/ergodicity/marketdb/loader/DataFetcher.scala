package com.ergodicity.marketdb.loader

import java.io.InputStream
import scalaz._
import Scalaz._
import scalaz.effects.IO

trait Cache {
  def cache: String
}

/**
 * Fetch market data from Data Reference
 */
trait DataFetcher[R <: DataRef] {
  def fetch(ref: R): String
  def toStream(ref: R): IO[InputStream]
}

object DataFetcher {
  implicit def InputStreamFetcher: DataFetcher[InputStreamRef] = new DataFetcher[InputStreamRef] {
    def fetch(ref: InputStreamRef) = "FETCH INPUT STREAM: " + ref

    def toStream(ref: InputStreamRef) = ref.is.pure[IO]
  }

  implicit def LocalFetcher: DataFetcher[LocalRef] = new DataFetcher[LocalRef] {
    def fetch(ref: LocalRef) = "FETCH LOCAL: " + ref

    def toStream(ref: LocalRef) = throw new UnsupportedOperationException
  }

  implicit def RemoteFetcher(implicit cache: Cache): DataFetcher[RemoteRef] = new DataFetcher[RemoteRef] {
    def fetch(ref: RemoteRef) = "FETCH REMOTE: " + ref + "; With cache: " + cache

    def toStream(ref: RemoteRef) = throw new UnsupportedOperationException
  }
}

