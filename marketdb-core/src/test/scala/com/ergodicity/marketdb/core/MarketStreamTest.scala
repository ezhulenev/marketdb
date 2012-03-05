package com.ergodicity.marketdb.core

import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import org.hbase.async.Scanner
import org.slf4j.LoggerFactory
import org.mockito.Mockito._
import org.junit.Test

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner]))
class MarketStreamTest {
  val log = LoggerFactory.getLogger(classOf[MarketStreamTest])

  @Test
  def testMarketStream() {
    val scanner = mock(classOf[Scanner])
    log.info("Scanner: "+scanner)
  }
}
