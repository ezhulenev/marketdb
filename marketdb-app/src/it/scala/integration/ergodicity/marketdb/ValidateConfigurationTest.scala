package integration.ergodicity.marketdb

import com.ergodicity.marketdb.MarketDbConfig
import com.stumbleupon.async.Deferred
import com.twitter.util.Eval
import java.io.File
import java.util.concurrent.TimeUnit
import org.hbase.async.{HBaseClient, TableNotFoundException}
import org.scalatest.WordSpec
import org.slf4j.LoggerFactory

class ValidateConfigurationTest extends WordSpec with EvalSupport {
  val log = LoggerFactory.getLogger(classOf[ValidateConfigurationTest])
  val DefaultTimeout = TimeUnit.SECONDS.toMillis(5)

  val configFile = new File("./config/it.scala")
  val eval = new Eval(getConfigTarget(configFile))
  val config = eval[MarketDbConfig](configFile)

  lazy val client = new HBaseClient(config.connection.zookeeperQuorum)

  "Test configuration" must {
    "load quorum properties" in {
      val quorum = config.connection.zookeeperQuorum
      log.info("Quorum: " + quorum)

      assert(quorum != null)
    }

    "connect to local HBase and fail on non-existing table" in {
      val exists: Deferred[AnyRef] = client.ensureTableExists("Test table")
      intercept[TableNotFoundException] {
        exists.join(DefaultTimeout)
      }
    }

    "connect to local HBase and verify test tables exists" in {
      client.ensureTableExists(config.tradesTable).join(DefaultTimeout)
      client.ensureTableExists(config.uidTable).join(DefaultTimeout)
    }
  }
}