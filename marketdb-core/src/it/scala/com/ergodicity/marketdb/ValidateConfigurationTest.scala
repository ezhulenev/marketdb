package com.ergodicity.marketdb

import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.slf4j.LoggerFactory
import org.scalatest.Spec
import java.util.concurrent.TimeUnit
import com.stumbleupon.async.Deferred
import org.hbase.async.TableNotFoundException

class ValidateConfigurationTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[ValidateConfigurationTest])
  val DefaultTimeout = TimeUnit.SECONDS.toMillis(5)

  val config = new AnnotationConfigApplicationContext(classOf[IntegrationTestConfiguration])

  describe("Test configuration") {

    it("should load quorum properties") {
      val quorum = config.getBean(classOf[IntegrationTestConfiguration]).quorumUrl
      log.info("Quorum: " + quorum)

      assert(quorum != null)
    }

    it("should connect to local HBase and fail on non-existing table") {
      val hbaseClient = config.getBean(classOf[IntegrationTestConfiguration]).hbaseClient()
      val exists: Deferred[AnyRef] = hbaseClient.ensureTableExists("Test table")
      intercept[TableNotFoundException] {
        exists.join(DefaultTimeout)
      }
    }

    it("should connect to local HBase and verify test tables exists") {
      val hbaseClient = config.getBean(classOf[IntegrationTestConfiguration]).hbaseClient()
      val tradesTableName = config.getBean(classOf[IntegrationTestConfiguration]).tradesTableName
      val uidTableName = config.getBean(classOf[IntegrationTestConfiguration]).uidTableName

      hbaseClient.ensureTableExists(tradesTableName).join(DefaultTimeout)
      hbaseClient.ensureTableExists(uidTableName).join(DefaultTimeout)
    }
  }
}