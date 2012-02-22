package com.ergodicity.marketdb.loader

import java.io.{InputStreamReader, BufferedReader, InputStream}
import com.ergodicity.marketdb.model.{Contract, Code, Market, TradePayload}
import org.joda.time.format.DateTimeFormat
import java.util.zip.{ZipEntry, ZipInputStream}

import scalaz.effects._
import scalaz._
import Scalaz._
import org.slf4j.LoggerFactory

trait DataParser[F] {
  def handleInputStream(is: InputStream): IO[Iterator[TradePayload]]
}

object DataParser {

  implicit def RtsTradeHistoryParser: DataParser[RtsTradeHistory] = new DataParser[RtsTradeHistory] {
    val log = LoggerFactory.getLogger(classOf[DataParser[RtsTradeHistory]].getName + "#RtsTradeHistoryParser");

    val RTS = Market("RTS")
    val DateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    val HeaderPrefix = "code;contract"
    val FuturesEntrySuffix = "ft.csv"
    val OptionsEntrySuffix = "ot.csv"

    def handleInputStream(is: InputStream) = buildIterator(is).pure[IO]

    private def buildIterator(is: InputStream): Iterator[TradePayload] = {
      val zis = new ZipInputStream(is)
      val reader = new BufferedReader(new InputStreamReader(zis))

      val tradeIterator: Iterator[TradePayload] = new Iterator[TradePayload] {
        var currentEntry: Option[ZipEntry] = None
        var currentPayload: Option[TradePayload] = None

        def hasNext: Boolean = {
          currentEntry <+> forceGetNextEntry map {
            _ =>
              currentPayload <+> forceGetNextPayload match {
                case None => currentEntry = None; hasNext // Current entry finished
                case Some(payload) => true
              }
          } match {
            case None => false
            case Some(r) => r
          }
        }

        def next() = if (hasNext) {
          val value = currentPayload.get
          currentPayload = None
          value
        } else null

        private def forceGetNextEntry() = {
          val entry = readNextZipEntry(zis)
          currentEntry = entry
          entry
        }

        private def forceGetNextPayload() = {
          val payload = readNextLine(reader) map createTradePayload
          currentPayload = payload
          payload
        }
      }

      tradeIterator
    }

    private def readNextZipEntry(zis: ZipInputStream): Option[ZipEntry] = {
      val nextEntry = zis.getNextEntry
      if (nextEntry == null) return None

      val acceptZipEntry = nextEntry.getName.endsWith(FuturesEntrySuffix) || nextEntry.getName.endsWith(OptionsEntrySuffix)
      if (acceptZipEntry) Some(nextEntry) else readNextZipEntry(zis)
    }

    private def readNextLine(reader: BufferedReader): Option[String] = {
      val line = reader.readLine()
      if (line == null) return None

      val acceptLine = !line.startsWith(HeaderPrefix) && line.trim().length() > 0;
      if (acceptLine) Some(line) else readNextLine(reader)
    }

    private def createTradePayload(line: String): TradePayload = {
      val split = line.split(";")

      val code = Code(split(0))
      val contract = Contract(split(1))
      val decimal = BigDecimal(split(2))
      val amount = split(3).toInt
      val time = DateFormat.parseDateTime(split(4))
      val tradeId = split(5).toLong
      val nosystem = split(6) == "1" // Nosystem	0 - Рыночная сделка, 1 - Адресная сделка

      TradePayload(RTS, code, contract, decimal, amount, time, tradeId, nosystem)
    }
  }
}