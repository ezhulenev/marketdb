package com.ergodicity.marketdb.loader

import org.scalatest.Spec
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpStatus, HttpClient}
import java.io.{IOException, File}
import org.slf4j.LoggerFactory
import org.joda.time.{LocalDate, DateTime}

class RefResolverTest extends Spec with HttpClientMatchers {
  val log = LoggerFactory.getLogger(classOf[RefResolverTest])

  val EmptyPattern = "empty"

  val RtsUrl = "http://ftp.rts.ru/pub/info/stats/history"
  val RtsPattern = "'/F/'YYYY'/FT'YYMMdd'.zip'"

  describe("Local Reference Resolver") {
    it("should throw exception on bad directory") {
      intercept[IllegalArgumentException] {
        RefResolver(new File("NoSuchDirecotry"), EmptyPattern)
      }
    }
  }

  describe("Remote reference resolver") {
    it("should return None if HTTP request throwed an Exception") {
      val client = mock(classOf[HttpClient])
      when(client.executeMethod(any(classOf[GetMethod]))).thenThrow(new IOException("Test"))

      val resolver = RefResolver(RtsUrl, RtsPattern)(client)
      val today = (new DateTime).toLocalDate

      assert(resolver.resolve(today).isEmpty)
    }

    it("should return None for error response code") {
      val client = mock(classOf[HttpClient])

      // Init mock
      when(client.executeMethod(any(classOf[GetMethod]))).thenReturn(HttpStatus.SC_NOT_FOUND)

      val resolver = RefResolver(RtsUrl, RtsPattern)(client)
      val today = (new DateTime).toLocalDate

      assert(resolver.resolve(today).isEmpty)
    }

    it("should return Some for success response code") {
      val client = mock(classOf[HttpClient])

      // Init mock
      when(client.executeMethod(any(classOf[GetMethod]))).thenReturn(HttpStatus.SC_OK)

      val resolver = RefResolver(RtsUrl, RtsPattern)(client)
      val today = new LocalDate(2012, 02, 01)

      val ref = resolver.resolve(today)
      log.info("Resolved reference: " + ref)
      assert(ref.isDefined)

      // -- Verify
      val expectedUrl: String = RtsUrl + "//F/2012/FT120201.zip"
      verify(client).executeMethod(getMethodFor(expectedUrl))
    }
  }
}