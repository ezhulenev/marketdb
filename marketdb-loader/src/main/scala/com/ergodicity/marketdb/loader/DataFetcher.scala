package com.ergodicity.marketdb.loader

/**
 * Fetch market data from Data Reference
 */
trait DataFetcher[R[_], F] {
  def fetch(implicit parser: DataParser[F]): String
}

object DataFetcher {
  implicit def LocalFetcher[F](ref: Local[F]): DataFetcher[Local, F] = new DataFetcher[Local, F] {
    def fetch(implicit parser: DataParser[F]) = "FETCH LOCAL; Parse: " + parser.read
  }

  implicit def RemoteFetcher[F](ref: Remote[F])(implicit cache: Cache): DataFetcher[Remote, F] = new DataFetcher[Remote, F] {
    def fetch(implicit parser: DataParser[F]) = "FETCH REMOTE; Cache: " + cache.cache + "; Parse: " + parser.read
  }
}


trait Cache {
  def cache: String
}