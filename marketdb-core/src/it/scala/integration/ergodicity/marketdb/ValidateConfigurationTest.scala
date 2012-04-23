package integration.ergodicity.marketdb

import org.slf4j.LoggerFactory
import org.scalatest.Spec
import java.util.concurrent.TimeUnit
import com.stumbleupon.async.Deferred
import java.io.File
import com.twitter.util.Eval
import org.hbase.async.{HBaseClient, TableNotFoundException}
import com.ergodicity.marketdb.core.MarketDBConfig

class ValidateConfigurationTest extends Spec with EvalSupport {
  val log = LoggerFactory.getLogger(classOf[ValidateConfigurationTest])
  val DefaultTimeout = TimeUnit.SECONDS.toMillis(5)

  val configFile = new File(this.getClass.getResource("/config/it.scala").toURI)
  val eval = new Eval(getConfigTarget(configFile))
  val config = eval[MarketDBConfig](configFile)

  lazy val client = new HBaseClient(config.zookeeperQuorum)

  describe("Test configuration") {
    it("should load quorum properties") {
      val quorum = config.zookeeperQuorum
      log.info("Quorum: " + quorum)

      assert(quorum != null)
    }

    it("should connect to local HBase and fail on non-existing table") {
      val exists: Deferred[AnyRef] = client.ensureTableExists("Test table")
      intercept[TableNotFoundException] {
        exists.join(DefaultTimeout)
      }
    }

    it("should connect to local HBase and verify test tables exists") {
      client.ensureTableExists(config.tradesTable).join(DefaultTimeout)
      client.ensureTableExists(config.uidTable).join(DefaultTimeout)
    }
  }
}