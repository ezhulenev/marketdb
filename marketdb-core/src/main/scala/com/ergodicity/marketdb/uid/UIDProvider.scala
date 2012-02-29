package com.ergodicity.marketdb.uid

import scala.concurrent.stm._
import org.hbase.async._
import scalaz._
import Scalaz._
import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.uid._
import java.util.ArrayList
import com.ergodicity.marketdb.AsyncHBase._
import scalaz.Digit._0
import com.twitter.util.FuturePool._
import java.util.concurrent.Executors
import com.twitter.util.{FuturePool, Promise, Future}
import com.ergodicity.marketdb.{OopsException, ByteArray, Oops}


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

  val ProvideIdThreadPoolSize = 1

  /**The single column family used by this class. */
  private val IdFamily = ByteArray('i', 'd')
  /**The single column family used by this class. */
  private val NameFamily = ByteArray('n', 'a', 'm', 'e')
  /**Row key of the special row used to track the max ID already assigned. */
  private val MaxIdRow = ByteArray(Array[Byte](0))
  /**Max attempts to execute put request*/
  private val MaxAttemptsCreateId = 3
  private val MaxAttemptsPut = 3

  val ProviderIdPool = FuturePool(Executors.newFixedThreadPool(ProvideIdThreadPoolSize))

  // Pending requests 
  private val pendingNames = Ref(Map[ByteArray, Future[Option[UniqueId]]]())
  private val pendingIds = Ref(Map[String, Future[Option[UniqueId]]]())
  private val pendingCreateIds = Ref(Map[String, Future[UniqueId]]())

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

  /**
   * Get id by given id, if exists some other pending request for given id, return it
   * After pending request finished, remove name from cache
   * @param id id to search
   * @return Future for Option[UniqueId] for given id
   */
  def getName(id: ByteArray) = {
    def releasePending() {
      atomic {
        implicit txn =>
          pendingNames.transform(_ - id)
      }
    }

    atomic {
      implicit txn =>
        pendingNames.single().get(id) getOrElse {
          val futureName = getNameInternal(id) {
            name =>
              val uid = name.map(UniqueId(_, id))
              uid.map(uid => cache.cache(uid.name, uid.id)).map({
                case Success(cached) => cached
                case Failure(err) => throw new RuntimeException("Failed to cache new UID: " + err)
              })
          }
          pendingNames.transform(_ + (id -> futureName))
          futureName onSuccess {_ => releasePending()} onFailure {_ => releasePending()}
          futureName
        }
    }
  }

  /**
   * Get id by given name, if exists some other pending request for given name, return it
   * After pending request finished, remove id from cache
   * @param name name to get id
   * @return Future for Option[UniqueId] for given name
   */
  def getId(name: String) = {
    def releasePending() {
      atomic {
        implicit txn =>
          pendingIds.transform(_ - name)
      }
    }

    atomic {
      implicit txn =>
        pendingIds.single().get(name) getOrElse {
          val futureId =  getIdInternal(name) { id =>
            val uid = id.map(UniqueId(name, _))
            uid.map(uid => cache.cache(uid.name, uid.id)).map({
              case Success(cached) => cached
              case Failure(err) => throw new RuntimeException("Failed to cache new UID: " + err)
            })
          }
          pendingIds.transform(_ + (name -> futureId))
          futureId onSuccess {_ => releasePending()} onFailure {_ => releasePending()}
          futureId
        }
    }
  }

  def provideId(name: String): Future[UniqueId] = {
    def releasePending() {
      atomic {
        implicit txn =>
          pendingCreateIds.transform(_ - name)
      }
    }

    def tryGetOrCreate = try {
      log.trace(" Try get or create id for name=" + name)
      getOrCreateId(name)
    } catch {
      case e: HBaseException => Oops("Create id faild: " + e).failNel[UniqueId]
    }

    atomic {
      implicit txn =>
        pendingCreateIds.single().get(name) getOrElse {
          val futureId =  ProviderIdPool(retryUntilValid(MaxAttemptsCreateId)(tryGetOrCreate _).fold(
            errors => throw new OopsException(errors),
            uid => uid
          ))
          pendingCreateIds.transform(_ + (name -> futureId))
          futureId onSuccess {_ => releasePending()} onFailure {_ => releasePending()}
          futureId
        }
    }
  }

  private def getNameInternal[R](id: ByteArray)(f: Option[String] => R): Future[R] = {
    // First check id width and fail if error occurred
    validateIdWidth(id)

    val cachedName = cache.name(id)
    cachedName match {
      case Some(_) => cacheHits += 1; Future {f(cachedName)}
      case None => cacheMisses += 1; getNameFromHBase(id)(opt => f(opt.map(_.asString)))
    }
  }

  private def getIdInternal[R](name: String)(f: Option[ByteArray] => R): Future[R] = {
    val cachedId = cache.id(name)
    cachedId match {
      case Some(_) => cacheHits += 1; Future {f(cachedId)}
      case None => cacheMisses += 1; getIdFromHBase(name)(f)
    }
  }

  private def getNameFromHBase[R](id: ByteArray)(f: Option[ByteArray] => R): Future[R] = {
    getHBaseValue(id, NameFamily)(f)
  }

  private def getIdFromHBase[R](name: String)(f: Option[ByteArray] => R): Future[R] = {
    val validation = getHBaseValue(ByteArray(name), IdFamily)(id => id)
    validation map (opt => f(opt.map(validateIdWidth(_))))
  }

  private def getHBaseValue[R](key: ByteArray, family: ByteArray, lock: Option[RowLock] = None)
                               (f: Option[ByteArray] => R): Future[R] = {

    val get = new GetRequest(table, key).family(family).qualifier(kind)
    lock.map(get.withRowLock(_))

    val deferred = client.get(get)

    val promise = new Promise[R]

    deferred addCallback {
      (row: ArrayList[KeyValue]) =>
        val value = if (row == null || row.isEmpty) f(None) else f(Some(ByteArray(row.get(0).value())))
        promise.setValue(value)
    }
    deferred addErrback {
      (e: Throwable) => promise.setException(e)
    }

    promise
  }

  private def validateIdWidth(id: ByteArray): ByteArray = {
    if (id.length == idWidth)
      id
    else
      throw new RuntimeException("Wrong id.length = " + id.length + " which is != " + idWidth + " required for '" + kind.asString + '\'')
  }

  private def passCheckOrFail[E, A](value: A)(validate: A => ValidationNEL[E, A], aggregatedErr: Option[NonEmptyList[E]] = None): A = {
    validate(value) match {
      case Success(suc) => suc
      case Failure(err) =>
        val aggregatedErrors = aggregatedErr.map(err <::: _) getOrElse err
        throw new RuntimeException(aggregatedErrors.toString())
    }
  }

  private def validateCurrentMaxIdLength(maxId: ByteArray): ValidationNEL[Oops, ByteArray] = {
    if (maxId.length == 8) {
      maxId.successNel[Oops]
    } else {
      Oops("Invalid currentMaxId=" + maxId.toString()).failNel[ByteArray]
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


  private def putToHBase(put: PutRequest): ValidationNEL[Oops, PutRequest] = {
    try {
      client.put(put).joinUninterruptibly()
      put.successNel[Oops]
    } catch {
      case e: HBaseException => Oops("HBase failed", Some(e)).failNel[PutRequest]
      case e => Oops("Failed put request: ", Some(e)).failNel[PutRequest]
    }
  }

  /**
   * We should validate that id value not greater then maximum allowed
   * and it's width equals to kind width
   * @param id generated id
   */
  private def validateGeneratedIdValue(id: ByteArray): ValidationNEL[Oops, ByteArray] = {
    // Verify that we're going to drop bytes that are 0.
    val leftBytesAreZero = id.slice(0, id.length - idWidth).foldLeft(true)((p, b) => p && (b == 0))
    val valid = if (leftBytesAreZero)
      id.successNel[Oops]
    else
      Oops("All Unique IDs for " + kind + " on " + idWidth + " bytes are already assigned!").failNel[ByteArray]

    valid
  }

  private def getOrCreateId(name: String): ValidationNEL[Oops, UniqueId] = {
    val uid: Option[ValidationNEL[Oops, UniqueId]] = getId(name).get().map(_.successNel[Oops])
    uid getOrElse {
        // Else try to ackquire row lock to create new id
        withRowLock {
          lock =>
          // Verify that the row still doesn't exist (to avoid re-creating it if
          // it got created before we acquired the lock due to a race condition).
            val uid = getId(name).get().map(_.successNel[Oops])

            uid getOrElse {
                // We verified that no one created id, so we need to do it here

                // Assign an ID.
                val (id, row) = getHBaseValue(MaxIdRow, IdFamily, Some(lock)) {
                  currentMaxId: Option[ByteArray] =>
                    val id = currentMaxId.map(existingMaxId => {
                      val bs = passCheckOrFail(existingMaxId)(validateCurrentMaxIdLength)
                      Bytes.getLong(bs.toArray) + 1
                    }) getOrElse 1l
                    val row = ByteArray(Bytes.fromLong(id))
                    (id, row)
                }.get()

                // Update Max id in HBase
                val updateMaxId = new PutRequest(table, MaxIdRow, IdFamily, kind, row, lock)
                passCheckOrFail(updateMaxId)(put => retryUntilValid(MaxAttemptsPut)(() => putToHBase(put)))
                log.trace("Generated id=" + id + " for kind='" + kind.asString + "' name='" + name + "'")

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

  private def withRowLock[R](f: RowLock => ValidationNEL[Oops, R]): ValidationNEL[Oops, R] = {
    val lock = client.lockRow(new RowLockRequest(table.toArray, MaxIdRow.toArray)).joinUninterruptibly()
    try {
      f(lock)
    } catch {
      case e: HBaseException => Oops("HBase failed", Some(e)).failNel[R]
      case e: Exception => Oops("Failed", Some(e)).failNel[R]
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

  def cache(name: String, id: ByteArray): ValidationNEL[Oops, UniqueId] = {
    val log = LoggerFactory.getLogger(classOf[UIDCache])
    atomic {
      implicit txn =>

        val nameValidation: Validation[Oops, String] = validateName(name, id)
        val idValidation: Validation[Oops, ByteArray] = validateId(id, name)

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
      case None => id.success[Oops]
      case Some(value) => if (value != null && value == name) {
        id.success[Oops]
      } else {
        Oops("id=" + id + " => name=" + name + ", already mapped to id " + value).fail[ByteArray]
      }
    }
  }

  private def validateName(name: String, id: ByteArray)(implicit txn: scala.concurrent.stm.InTxn) = {
    nameCache() get (name) match {
      case None => name.success[Oops]
      case Some(value) => if (value != null && value == id) {
        name.success[Oops]
      } else {
        Oops("name=" + name + " => id=" + id + ", already mapped to name " + value).fail[String]
      }
    }
  }
}


