package com.ergodicity.marketdb.loader

/**
 * Fetch market data from Data Reference
 */
trait DataFetcher[R[_]] {
  def fetch[F](ref: R[F]): String
}

object DataFetcher {
  implicit def LocalFetcher: DataFetcher[Local] = new DataFetcher[Local] {
    def fetch[F](ref: Local[F]) = "FETCH LOCAL"
  }

  implicit def RemoteFetcher: DataFetcher[Remote] = new DataFetcher[Remote] {
    def fetch[F](ref: Remote[F]) = "FETCH REMOTE"
  }
}
