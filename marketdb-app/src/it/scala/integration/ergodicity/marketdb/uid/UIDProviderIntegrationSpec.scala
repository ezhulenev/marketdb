package integration.ergodicity.marketdb.uid

import com.ergodicity.marketdb.uid.{UIDProvider, UniqueId, UIDCache}
import com.ergodicity.marketdb.{MarketDbConfig, ByteArray}
import com.twitter.util.Eval
import integration.ergodicity.marketdb.{TimeRecording, EvalSupport}
import java.io.File
import java.lang.StringBuffer
import java.util.Random
import org.scalatest.{WordSpec, GivenWhenThen}
import org.slf4j.LoggerFactory

class UIDProviderIntegrationSpec extends WordSpec with GivenWhenThen with TimeRecording with EvalSupport {
  val log = LoggerFactory.getLogger(classOf[UIDProviderIntegrationSpec])

  val Characters = "QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm1234567890"
  val Kind = "TestKind"

  val RandomGenerator = new Random

  val configFile = new File("./config/it.scala")
  val eval = new Eval(getConfigTarget(configFile))
  val config = eval[MarketDbConfig](configFile)


  implicit val connectionBuilder = config.HBaseClientBuilder

  "UIDProvider" must {

    "should return None for unknown name" in {
      val unknownName = generateString(10) // Generate unique name

      given("new UIDProvider with empty cache")
      val provider = createNewProvider

      when("send GetId request")
      val uid = recordTime("Request for id", () => provider.getId(unknownName).get())

      then("None should be returned")
      log.info("Unique id: " + uid)

      assert(uid match {
        case None => true
        case _ => false
      })
    }

    "should create new id for given name" in {
      val name = generateString(10) // Generate unique name

      given("new UIDProvider with empty cache")
      val provider = createNewProvider

      when("send GetOrCreate request")
      val uid = recordTime("Create new id", () => provider.provideId(name)).get()

      then("new UniqueID should be generated")
      log.info("Unique id: " + uid)

      // Verify name equals
      assert(uid match {
        case UniqueId(n, i) => n == name
        case _ => false
      })

      and("GetId should return generated id")
      val gotId = recordTime("Get generated id by name", () => provider.getId(name).get())
      log.info("Got id: " + gotId)

      assert(gotId match {
        case Some(UniqueId(n, i)) => n == name && i == uid.id
        case _ => false
      })

      and("GetName should return initial name")
      val gotName = recordTime("Get name by generated id", () => provider.getName(uid.id).get())
      log.info("Got name: " + gotName)

      assert(gotName match {
        case Some(UniqueId(n, i)) => n == name && i == uid.id
        case _ => false
      })
    }
  }

  def createNewProvider = {
    val cache = new UIDCache
    new UIDProvider(config.connection, cache, ByteArray(config.uidTable), ByteArray(Kind), 3)
  }

  def generateString(length: Int) = {
    val buff = new StringBuffer()
    for (i <- 1 to length) {
      buff.append(Characters.charAt(RandomGenerator.nextInt(Characters.length)))
    }
    buff.toString
  }


}