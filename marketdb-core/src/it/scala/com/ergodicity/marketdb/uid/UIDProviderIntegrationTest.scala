package com.ergodicity.marketdb.uid

import scalaz._
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import java.util.Random
import java.lang.StringBuffer
import org.scalatest.{GivenWhenThen, Spec}
import com.ergodicity.marketdb.{TimeRecording, ByteArray, IntegrationTestConfiguration}

class UIDProviderIntegrationTest extends Spec with GivenWhenThen with TimeRecording {

  val Characters = "QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm1234567890"
  val Kind = "TestKind"

  val RandomGenerator = new Random

  val config = new AnnotationConfigApplicationContext(classOf[IntegrationTestConfiguration])


  describe("UIDProvider") {

    it("should return None for unknown name") {
      val unknownName = generateString(10) // Generate unique name

      given("new UIDProvider with empty cache")
      val provider = createNewProvider

      when("send GetId request")
      val uid = recordTime("Request for id", () => provider.findId(unknownName))

      then("None should be returned")
      log.info("Unique id: " + uid)

      assert(uid match {
        case Success(None) => true
        case _ => false
      })
    }

    it("should create new id for given name") {
      val name = generateString(10) // Generate unique name

      given("new UIDProvider with empty cache")
      val provider = createNewProvider

      when("send GetOrCreate request")
      val uid = recordTime("Create new id", () => provider.provideId(name))

      then("new UniqueID should be generated")
      log.info("Unique id: " + uid)

      // Verify name equals
      assert(uid match {
        case Success(UniqueId(n, i)) => n == name
        case _ => false
      })

      val generatedUid = uid.toOption.get

      and("GetId should return generated id")
      val gotId = recordTime("Get generated id by name", () => provider.findId(name))
      log.info("Got id: " + gotId)

      assert(gotId match {
        case Success(Some(UniqueId(n, i))) => n == name && i == generatedUid.id
        case _ => false
      })

      and("GetName should return initial name")
      val gotName = recordTime("Get name by generated id", () => provider.findName(generatedUid.id))
      log.info("Got name: " + gotName)

      assert(gotName match {
        case Success(Some(UniqueId(n, i))) => n == name && i == generatedUid.id
        case _ => false
      })
    }
  }

  def createNewProvider = {
    val cache = new UIDCache
    val hbaseClient = config.getBean(classOf[IntegrationTestConfiguration]).hbaseClient()
    val uidTableName = config.getBean(classOf[IntegrationTestConfiguration]).uidTableName

    new UIDProvider(hbaseClient, cache, ByteArray(uidTableName), ByteArray(Kind), 3)
  }

  def generateString(length: Int) = {
    val buff = new StringBuffer()
    for (i <- 1 to length) {
      buff.append(Characters.charAt(RandomGenerator.nextInt(Characters.length)))
    }
    buff.toString
  }


}