package com.ergodicity.marketdb.loader

import org.joda.time.LocalDate
import scalaz._
import Scalaz._
import scalaz.IterV
import com.ergodicity.marketdb.model.TradePayload
import java.io.File
import org.apache.commons.httpclient.HttpClient

trait TradeLoader {
  def enumTrades[A](day: LocalDate, i: IterV[TradePayload, A]): Option[A]
}

class RtsTradeLoader(dir: File, url: String, pattern: String) extends TradeLoader {
  implicit val client = new HttpClient()
  implicit val cache = new LocalMirrorCache(dir, url)

  val localResolver = new TradeResolver(RefResolver(dir, pattern), RtsTradeHistory(_: LocalRef))
  val ftpResolver = new TradeResolver(RefResolver(url, pattern), RtsTradeHistory(_: RemoteRef))

  def enumTrades[A](day: LocalDate, i: IterV[TradePayload, A]) = {
    import com.ergodicity.marketdb.loader.TradeDataIteratee._

    val local = localResolver.resolve(day).map {
      _.enumTradeData(i)
    }

    def ftp = {
      ftpResolver.resolve(day).map {
        _.enumTradeData(i)
      }
    }

    (local <+> ftp).map(_.unsafePerformIO.run)
  }
}
