package com.ergodicity.marketdb.uid

import scala.concurrent.stm._
import org.hbase.async._
import scalaz._
import Scalaz._
import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.uid._
import com.ergodicity.marketdb.{ByteArray, Ooops}


/**
 * Represents a provider of Unique IDs, manages the lookup and creation of IDs.
 *
 * For efficiency, various kinds of "names" need to be mapped to small, unique
 * IDs.  For instance, we give a unique ID to each stock symbol etc.
 * An instance of this class handles the unique IDs for one kind of ID.
 *
 * IDs are looked up in HBase and cached forever in memory (since they're
 * immutable).  IDs are encoded on a fixed number of bytes, which is
 * implementation dependent.
 */
class UIDProvider(client: HBaseClient, cache: UIDCache,
                  table: ByteArray, kind: ByteArray, idWidth: Short) {

  private val log = LoggerFactory.getLogger(classOf[UIDProvider])

  /** The single column family used by this class. */
  private val IdFamily = ByteArray('i','d')
  /** The single column family used by this class. */
  private val NameFamily = ByteArray('n','a','m','e')
  /** Row key of the special row used to track the max ID already assigned. */
  private val MaxIdRow = ByteArray(Array[Byte](0))
  /** Max attempts to execute put request*/
  private val MaxAttemptsCreateId = 5
  private val MaxAttemptsPut = 5

  val PutRetryCount = 10

  if (kind.isEmpty) {
    throw new IllegalArgumentException("Empty string as 'kind' argument!")
  }
  if (table.isEmpty) {
    throw new IllegalArgumentException("Empty string as 'table' argument!")
  }
  if (idWidth < 1 || idWidth > 8) {
    throw new IllegalArgumentException("Invalid width: " + idWidth)
  }

  /**Number of times we avoided reading from HBase thanks to the cache. */
  @volatile
  var cacheHits: Int = 0

  /**Number of times we had to read from HBase and populate the cache. */
  @volatile
  var cacheMisses: Int = 0

  def findName(id: ByteArray) = {
    log.info("Get name: " + id)
    withName(id)(_.map(UniqueId(_, id)))
  }

  def findId(name: String) = {
    log.info("Get id: " + name)
    withId(name)(_.map(UniqueId(name, _)))
  }

  def provideId(name: String) = {
    val tryGetOrCreate = () => try {
      log.debug(" Try get or create id for name="+name)
      getOrCreateId(name)
    } catch {
      case e: HBaseException => Ooops("Create id faild: " + e).failNel[UniqueId]
    }

    retryUntilValid(MaxAttemptsCreateId)(tryGetOrCreate)
  }

  private def withName(id: ByteArray)(f: Option[String] => Option[UniqueId]): ValidationNEL[Ooops, Option[UniqueId]] = {
    // First check id width and fail if error occured
    passCheckOrFail(id)(validateIdWidth)

    val cachedName = cache.name(id)
    cachedName match {
      case Some(_) => cacheHits += 1; f(cachedName).successNel[Ooops]
      case None =>
        cacheMisses += 1;
        val uid = withNameFromHBase(id)(opt => f(opt.map(_.asString)))
        // Add to cache with validation
        uid.flatMap(_.map(newUid => cache.cache(newUid.name, newUid.id)).map(_.map(Some(_))) getOrElse uid)
    }
  }

  private def withId(name: String)(f: Option[ByteArray] => Option[UniqueId]): ValidationNEL[Ooops, Option[UniqueId]] = {
    val cachedId = cache.id(name)
    cachedId match {
      case Some(_) => cacheHits += 1; f(cachedId).successNel[Ooops]
      case None =>
        cacheMisses += 1;
        val uid = withIdFromHBase(name)(f)
        // Add to cache with validation
        uid.flatMap(_.map(newUid => cache.cache(newUid.name, newUid.id)).map(_.map(Some(_))) getOrElse uid)
    }
  }

  private def withNameFromHBase[R](id: ByteArray)(f: Option[ByteArray] => R): ValidationNEL[Ooops, R] = {
    withHBaseValue(id, NameFamily)(f)
  }

  private def withIdFromHBase[R](name: String)(f: Option[ByteArray] => R): ValidationNEL[Ooops, R] = {
    val validation = withHBaseValue(ByteArray(name), IdFamily)(id => id)
    val validatedWidth = validation.flatMap(_.map(validateIdWidth _).map(_.map(Some(_))) getOrElse validation)

    validatedWidth.map(f)
  }

  private def withHBaseValue[R](key: ByteArray, family: ByteArray, lock: Option[RowLock] = None)
                               (f: Option[ByteArray] => R): ValidationNEL[Ooops, R] = {
    val get = new GetRequest(table, key).family(family).qualifier(kind)
    lock.map(get.withRowLock(_))

    try {
      val row = client.get(get).joinUninterruptibly
      val value = if (row == null || row.isEmpty) f(None) else f(Some(ByteArray(row.get(0).value())))
      value.successNel[Ooops]
    } catch {
      case e: HBaseException => Ooops("HBase failed", Some(e)).failNel[R]
      case e: Exception => Ooops("HBase 'get' failed", Some(e)).failNel[R]
    }
  }

  private def validateIdWidth(id: ByteArray): ValidationNEL[Ooops, ByteArray] = {
    if (id.length == idWidth)
      id.successNel[Ooops]
    else
      Ooops("Wrong id.length = " + id.length + " which is != " + idWidth + " required for '" + kind + '\'').failNel[ByteArray]
  }

  private def passCheckOrFail[E, A](value: A)(validate: A => ValidationNEL[E, A], aggregatedErr: Option[NonEmptyList[E]] = None): A = {
    validate(value) match {
      case Success(suc) => suc
      case Failure(err) => 
        val aggregatedErrors = aggregatedErr.map(err <::: _) getOrElse err
        throw new RuntimeException(aggregatedErrors.toString())
    }
  }

  private def validateCurrentMaxIdLength(maxId: ByteArray): ValidationNEL[Ooops, ByteArray] = {
    if (maxId.length == 8) {
      maxId.successNel[Ooops]
    } else {
      Ooops("Invalid currentMaxId=" + maxId.toString()).failNel[ByteArray]
    }
  }

  def retryUntilValid[E, A](retryCount: Int)
                           (computation: () => ValidationNEL[E, A]): ValidationNEL[E, A] = {
    val errSuccessStreams = Stream.iterate(computation())(prev => {
      val curr = computation()
      prev <+> curr
    }) take retryCount span (_.isFailure)
    (errSuccessStreams._2 ++ errSuccessStreams._1.reverse) head
  }


  private def putToHBase(put: PutRequest): ValidationNEL[Ooops, PutRequest] = {
    try {
      client.put(put).joinUninterruptibly()
      put.successNel[Ooops]
    } catch {
      case e: HBaseException => Ooops("HBase failed", Some(e)).failNel[PutRequest]
      case e => Ooops("Failed put request: ", Some(e)).failNel[PutRequest]
    }
  }

  /**
   * We should validate that id value not greater then maximum allowed
   * and it's width equals to kind width
   * @param id generated id
   */
  private def validateGeneratedIdValue(id: ByteArray): ValidationNEL[Ooops, ByteArray] = {

    // Verify that we're going to drop bytes that are 0.
    val leftBytesAreZero = id.slice(0, id.length - idWidth).foldLeft(true)((p, b) => p && (b == 0))
    val valid = if (leftBytesAreZero)
      id.successNel[Ooops]
    else
      Ooops("All Unique IDs for " + kind + " on " + idWidth + " bytes are already assigned!").failNel[ByteArray]

    validateIdWidth(id) <+> valid
  }

  private def getOrCreateId(name: String): ValidationNEL[Ooops, UniqueId] = {
    val uid: ValidationNEL[Ooops, Option[UniqueId]] = withId(name)(_.map(id => UniqueId(name, id)))

    uid flatMap {
      _.map(_.successNel[Ooops]) getOrElse {
        // Else try to ackquire row lock to create new id
        withRowLock {
          lock =>
          // Verify that the row still doesn't exist (to avoid re-creating it if
          // it got created before we acquired the lock due to a race condition).
            val uid = withId(name)(_.map(id => UniqueId(name, id)))
            uid flatMap {
              _.map(_.successNel[Ooops]) getOrElse {
                // We verified that no one created id, so we need to do it here

                // Assign an ID.
                val assignedId = withHBaseValue(MaxIdRow, IdFamily, Some(lock)) {
                  currentMaxId: Option[ByteArray] =>
                    val id = currentMaxId.map(existingMaxId => {
                      val bs = passCheckOrFail(existingMaxId)(validateCurrentMaxIdLength)
                      Bytes.getLong(bs.toArray) + 1
                    }) getOrElse 1l
                    val row = ByteArray(Bytes.fromLong(id))
                    (id, row)
                }

                val (id, row) = assignedId match {
                  case Success((id, row)) => (id, row)
                  case Failure(e) => throw new RuntimeException("Can't assing new id: "+e)
                }

                // Update Max id in HBase
                val updateMaxId = new PutRequest(table, MaxIdRow, IdFamily, kind, row, lock)
                passCheckOrFail(updateMaxId)(put => retryUntilValid(MaxAttemptsPut)(() => putToHBase(put)))
                log.info("Generated id=" + id + " for kind='" + kind.asString + "' name='" + name + "'")

                // Validate generated row length && first (row.length - idWidth) bytes
                passCheckOrFail(row)(validateGeneratedIdValue)

                // Shrink the ID on the requested number of bytes.
                val shrinkRow = row.slice(row.length - idWidth, row.length)

                // If we die before the next PutRequest succeeds, we just waste an ID.
                // Create the reverse mapping first, so that if we die before creating
                // the forward mapping we don't run the risk of "publishing" a
                // partially assigned ID.  The reverse mapping on its own is harmless
                // but the forward mapping without reverse mapping is bad.
                val reverseMapping = new PutRequest(table, shrinkRow, NameFamily.toArray, name, name)
                passCheckOrFail(reverseMapping)(
                  validate = (put: PutRequest) => retryUntilValid(MaxAttemptsPut)(() => putToHBase(put)),
                  aggregatedErr = Some(NonEmptyList("Failed to Put reverse mapping!  ID leaked: " + id))
                )

                // Now create the forward mapping.
                val forwardMapping: PutRequest = new PutRequest(table, name, IdFamily, kind, shrinkRow)
                passCheckOrFail(forwardMapping)(
                  validate = (put: PutRequest) => retryUntilValid(MaxAttemptsPut)(() => putToHBase(put)),
                  aggregatedErr = Some(NonEmptyList("Failed to Put forward mapping!  ID leaked: " + id))
                )

                cache.cache(name, shrinkRow)
              }
            }
        }
      }
    }
  }

  private def withRowLock[R](f: RowLock => ValidationNEL[Ooops, R]): ValidationNEL[Ooops, R] = {
    val lock = client.lockRow(new RowLockRequest(table.toArray, MaxIdRow.toArray)).joinUninterruptibly()
    try {
      f(lock)
    } catch {
      case e: HBaseException => Ooops("HBase failed", Some(e)).failNel[R]
      case e: Exception => Ooops("Failed", Some(e)).failNel[R]
    } finally {
      client.unlockRow(lock)
    }
  }

  override def toString = "UIDProvider(" + table + ", " + kind + ", " + idWidth + ")"
}

class UIDCache {
  /**Cache for forward mappings (name to ID). */
  private val nameCache = Ref(Map[String, ByteArray]())

  /**Cache for backward mappings (ID to name). */
  private val idCache = Ref(Map[ByteArray, String]())

  def name(id: ByteArray) = idCache.single() get (id)

  def id(name: String) = nameCache.single() get (name)

  def cachedNames = nameCache.single().keySet

  def cachedIds = idCache.single().keySet

  def cache(name: String, id: ByteArray): ValidationNEL[Ooops, UniqueId] = {
    atomic {
      implicit txn =>

        val nameValidation: Validation[Ooops, String] = validateName(name, id)
        val idValidation: Validation[Ooops, ByteArray] = validateId(id, name)

        (nameValidation.liftFailNel |@| idValidation.liftFailNel) {
          (validaName, validId) =>
            idCache transform (_ + (validId -> validaName))
            nameCache transform (_ + (validaName -> validId))
            UniqueId(name, id)
        }
    }
  }

  private def validateId(id: ByteArray, name: String)(implicit txn: scala.concurrent.stm.InTxn) = {
    idCache() get (id) match {
      case None => id.success[Ooops]
      case Some(value) => if (value != null && value == name) {
        id.success[Ooops]
      } else {
        Ooops("id=" + id + " => name=" + name + ", already mapped to id " + value).fail[ByteArray]
      }
    }
  }

  private def validateName(name: String, id: ByteArray)(implicit txn: scala.concurrent.stm.InTxn) = {
    nameCache() get (name) match {
      case None => name.success[Ooops]
      case Some(value) => if (value != null && value == id) {
        name.success[Ooops]
      } else {
        Ooops("name=" + name + " => id=" + id + ", already mapped to name " + value).fail[String]
      }
    }
  }
}


