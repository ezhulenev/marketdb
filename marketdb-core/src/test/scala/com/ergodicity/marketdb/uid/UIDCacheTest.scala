package com.ergodicity.marketdb.uid

import scalaz._
import org.slf4j.LoggerFactory
import org.scalatest.{GivenWhenThen, Spec}
import com.ergodicity.marketdb.ByteArray

class UIDCacheTest extends Spec with GivenWhenThen {
  val log = LoggerFactory.getLogger(classOf[UIDCacheTest])

  describe("UIDCache") {

    it("should return None if no value cached for given name") {
      given("an empty cache")
      val cache = new UIDCache

      when("tring to get id by name")
      val id = cache.id("NoSuchName")

      then("None should be return")
      id match {
        case None => assert(true)
        case _ => assert(false)
      }
    }

    it("should return None if no value cached for given id") {
      given("an empty cache")
      val cache = new UIDCache

      when("tring to get name by id")
      val name = cache.name(ByteArray(Array[Byte](0)))

      then("None should be return")
      name match {
        case None => assert(true)
        case _ => assert(false)
      }
    }

    it("should properly return cached values") {
      given("non empty cache")
      val name = "Name"
      val id = ByteArray(Array[Byte](0))
      val cache = new UIDCache
      cache.cache(name, id)

      when("trying to get id by name")
      val idLoaded = cache.id(name)

      then("it should return valid id")
      assert(idLoaded.map(_ == id) getOrElse false)

      when("trying to get name by if")
      val nameLoaded = cache.name(id)

      then("it should return valid name")
      assert(nameLoaded.map(_ == name) getOrElse false)
    }
  }

  it("should successfully cache the same values twice") {
    given("non empty cache")
    val name = "Name"
    val id = ByteArray(Array[Byte](0))
    val cache = new UIDCache
    cache.cache(name, id)

    when("adding values already presented in cache")
    val cached = cache.cache(name, id)

    then("it should succesfully cache the scame pair second time")
    cached match {
      case Success(_) => assert(true)
      case _ => assert(false)
    }

    and("internal cache sizes should be equals to one")
    assert(cache.cachedNames.size == 1)
    assert(cache.cachedIds.size == 1)
  }

  it("should fail to cache different ids with same name") {
    given("non empty cache")
    val name = "Name"
    val id1 = ByteArray(Array[Byte](0))
    val id2 = ByteArray(Array[Byte](1))
    val cache = new UIDCache
    cache.cache(name, id1)

    when("adding value with name already cached and other id")
    val cached = cache.cache(name, id2)

    then("should fail")
    cached match {
      case Failure(errors) => log.info("Validaion: " + errors); assert(true)
      case _ => assert(false)
    }

    and("internal cache sizes should be equals to one")
    assert(cache.cachedNames.size == 1)
    assert(cache.cachedIds.size == 1)
  }

  it("should fail to cache different names with same id") {
    given("non empty cache")
    val name1 = "Name1"
    val name2 = "Name2"
    val id = ByteArray(Array[Byte](0))
    val cache = new UIDCache
    cache.cache(name1, id)

    when("adding value with id already cached and other name")
    val cached = cache.cache(name2, id)

    then("should fail")
    cached match {
      case Failure(errors) => log.info("Validaion: " + errors); assert(true)
      case _ => log.info("Actual result is: " + cached); assert(false)
    }

    and("internal cache sizes should be equals to one")
    assert(cache.cachedNames.size == 1)
    assert(cache.cachedIds.size == 1)
  }


}
