package com.ergodicity.marketdb.loader

import org.slf4j.LoggerFactory
import org.scalatest.WordSpec
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpStatus, HttpClient}
import org.joda.time.{LocalDate, DateTime}
import util.Iteratees
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

class RemoteTradeResolverTest extends WordSpec with HttpClientMatchers {
  val log = LoggerFactory.getLogger(classOf[RemoteTradeResolverTest])

  val is = classOf[RemoteTradeResolverTest].getResourceAsStream("/data/FT120201.zip")

  val RtsFtpUrl = "http://ftp.rts.ru/pub/info/stats/history"
  val RtsPattern = "'/F/'YYYY'/FT'YYMMdd'.zip'"

  implicit val cache = new RemoteFetcherCache {
    def cache(ref: RemoteRef, is: InputStream) = {
      import scalax.io._
      val out = new ByteArrayOutputStream
      Resource.fromInputStream(is) copyDataTo Resource.fromOutputStream(out)
      new ByteArrayInputStream(out.toByteArray)
    }
  }

  implicit val client = mock(classOf[HttpClient])

  val RemoteRefResolver = RefResolver(RtsFtpUrl, RtsPattern)

  "Remote RTS History Resolver" must {

    val tradeResolver = new TradeResolver(RemoteRefResolver, RtsTradeHistory(_: RemoteRef))

    "return None for non existing trade data" in {
      reset(client)
      when(client.executeMethod(any(classOf[GetMethod]))).thenReturn(HttpStatus.SC_NOT_FOUND)

      val tradeData = tradeResolver.resolve((new DateTime).toLocalDate)
      assert(tradeData.isEmpty)
    }

    "return Some for existing trade data" in {
      // -- Set response stream to test data
      reset(client)
      when(client.executeMethod(headMethodFor(RtsFtpUrl+"//F/2012/FT120201.zip"))).thenReturn(HttpStatus.SC_OK)
      when(client.executeMethod(getMethodFor(RtsFtpUrl+"//F/2012/FT120201.zip", is))).thenReturn(HttpStatus.SC_OK)

      val date = new LocalDate(2012, 2, 1)
      val tradeData = tradeResolver.resolve(date)
      log.info("Trade data: " + tradeData)
      assert(tradeData.isDefined)

      import TradeDataIteratee._
      import Iteratees._

      // Count Trade Data
      val count = tradeData.map(_.enumTradeData(counter)).map(_.unsafePerformIO).map(_.run).get
      log.info("Count: "+count)
      assert(count == 60)
    }
  }

}
