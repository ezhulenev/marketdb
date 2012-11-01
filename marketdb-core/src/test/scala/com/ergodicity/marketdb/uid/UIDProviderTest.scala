package com.ergodicity.marketdb.uid

import com.ergodicity.marketdb.Oops
import com.ergodicity.marketdb._
import com.stumbleupon.async.{Callback, Deferred}
import java.util.{Arrays, ArrayList}
import org.hbase.async._
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.powermock.core.classloader.annotations.{PowerMockIgnore, PrepareForTest}
import org.powermock.modules.junit4.PowerMockRunner
import org.scalatest.Assertions._
import org.slf4j.LoggerFactory
import scala.Some
import scalaz._
import scalaz.Scalaz._

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[HBaseClient], classOf[RowLock], classOf[Deferred[_]]))
class UIDProviderTest extends HBaseMatchers {
  val log = LoggerFactory.getLogger(classOf[UIDProviderTest])

  val IdFamily = "Id"
  val Table = "SomeTable"
  val Kind = "SomeKind"
  private val MaxIdRow = ByteArray(Array[Byte](0))

  @Test
  def testNameSuccessfulHBaseLookup() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val cache = new UIDCache
    val provider = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)

    val id = ByteArray(Array[Byte](0, 0, 1))
    val name = "Name"

    val kvs = new ArrayList[KeyValue](1)
    kvs.add(new KeyValue(id, IdFamily, Kind, name))

    when(client.get(anyGet)).thenReturn(Deferred.fromResult(kvs))

    var response = provider.getName(id).get()
    log.info("Response: " + response)

    assert(responseIsValid(response))
    
    Thread.sleep(1000)
    // Should be a cache hit ...
    response = provider.getName(id).get()
    assert(responseIsValid(response))

    assertEquals(1, provider.cacheHits)
    assertEquals(1, provider.cacheMisses)
    assertEquals(1, cache.cachedIds.size)
    assertEquals(1, cache.cachedNames.size)

    verify(client, only()).get(anyGet)

    def responseIsValid = {response: Option[UniqueId] =>
      log.info("Check response: "+response)
      response match {
        case Some(UniqueId(n, i)) => i == id && n == name
        case _ => false
      }
    }
  }

  @Test
  def testNameWithErrorDuringHBaseLookup() {

    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val cache = new UIDCache
    val provider = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)

    val id = ByteArray("123")
    val name = "Name"

    val kvs = new ArrayList[KeyValue](1)
    kvs.add(new KeyValue(id, IdFamily, Kind, name))

    val hbe = mock(classOf[HBaseException])
    when(client.get(anyGet)).thenThrow(hbe).thenReturn(Deferred.fromResult(kvs))

    // First request should fails
    intercept[HBaseException] {
      provider.getName(id)
    }

    // Second request should be success
    val response = provider.getName(id).get()
    log.info("Response#1: " + response)
    assert(response match {
      case Some(UniqueId(n, i)) => n == name && i == id
      case _ => false
    })

    // Verify post execution results
    assertEquals(0, provider.cacheHits)
    assertEquals(2, provider.cacheMisses)
    assertEquals(1, cache.cachedIds.size)
    assertEquals(1, cache.cachedNames.size)

    // Verify client execution
    verify(client, times(2)).get(anyGet)
  }

  @Test
  def testNameWithErrorDuringDeferedCallback() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val cache = new UIDCache
    val uid = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)

    val id = ByteArray("123")
    val name = "Name"

    val kvs = new ArrayList[KeyValue](1)
    kvs.add(new KeyValue(id, IdFamily, Kind, name))

    val error = new RuntimeException("Some test error")
    when(client.get(anyGet))
      .thenReturn(Deferred.fromError[ArrayList[KeyValue]](error))   // First invocation fails on Defered
      .thenReturn(Deferred.fromResult(kvs))                         // Second returns valid result

    // First request should fail
    intercept[RuntimeException] {
      uid.getName(id).get()
    }

    // Second request should be success
    val response = uid.getName(id).get()
    log.info("Response#1: " + response)
    response match {
      case Some(UniqueId(uName, uId)) => {
        assert(uName == name)
        assert(uId == id)
      }
      case _ => assert(false)
    }

    // Verify post execution results
    assertEquals(0, uid.cacheHits)
    assertEquals(2, uid.cacheMisses)
    assertEquals(1, cache.cachedIds.size)
    assertEquals(1, cache.cachedNames.size)

    // Verify client execution
    verify(client, times(2)).get(anyGet)
  }

  @Test
  def testNameWithErrorDuringAddingToCache() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val cache = mock(classOf[UIDCache])
    val provider = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)

    val id = ByteArray("123")
    val name = "Name"

    val kvs = new ArrayList[KeyValue](1)
    kvs.add(new KeyValue(id, IdFamily, Kind, name))

    // Setup mocks
    when(client.get(anyGet))                  // Both HBase lookups are successful
      .thenReturn(Deferred.fromResult(kvs))
      .thenReturn(Deferred.fromResult(kvs))

    when(cache.name(any(classOf[ByteArray]))).thenReturn(None)
    when(cache.cache(anyString(), any(classOf[ByteArray])))
        .thenReturn(Oops("Error during adding values to cache").failNel[UniqueId])  // First cache fails
        .thenReturn(UniqueId(name, id).successNel[Oops]);        // Second success

    // First request should fail
    intercept[RuntimeException] {
      provider.getName(id).get()
    }

    // Second request should be success
    val response = provider.getName(id).get()
    log.info("Response#1: " + response)
    response match {
      case Some(UniqueId(uName, uId)) => {
        assert(uName == name.toString)
        assert(uId == id)
      }
      case _ => assert(false)
    }

    // Verify post execution results
    assertEquals(0, provider.cacheHits)
    assertEquals(2, provider.cacheMisses)

    // Verify client execution
    verify(client, times(2)).get(anyGet)
    verify(cache, times(2)).cache(anyString(), any(classOf[ByteArray]))
  }

  @Test
  def testNameForNonexistentId() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val provider = new UIDProvider(marketDbClient, new UIDCache, ByteArray(Table), ByteArray(Kind), 3)
    when(client.get(anyGet)).thenReturn(Deferred.fromResult(new ArrayList[KeyValue](0)))

    val noSuchId = ByteArray("123")

    val response = provider.getName(noSuchId).get()
    log.info("Response: " + response)
    response match {
      case None => assert(true)
      case _ => assert(false)
    }
  }

  @Test
  def testNameWithInvalidId() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val provider = new UIDProvider(marketDbClient, new UIDCache, ByteArray(Table), ByteArray(Kind), 3)
    when(client.get(anyGet)).thenReturn(Deferred.fromResult(new ArrayList[KeyValue](0)))

    val invalidId = ByteArray("TooLongId")

    intercept[RuntimeException] {
      provider.getName(invalidId)
    }
  }

  @Test
  def testIdSuccessfulHBaseLookup() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val cache = new UIDCache
    val provider = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)

    val id = ByteArray("123")
    val name = "Name"

    val kvs = new ArrayList[KeyValue](1)
    kvs.add(new KeyValue(name, IdFamily, Kind, id))

    when(client.get(anyGet)).thenReturn(Deferred.fromResult(kvs))

    // First request should be propagated to HBase
    var response = provider.getId(name).get()
    assert(responseIsValid(response))

    // Second should be a cache hit ...
    response = provider.getId(name).get()
    assert(responseIsValid(response))

    // Validate cache state
    assertEquals(1, provider.cacheHits)
    assertEquals(1, provider.cacheMisses)
    assertEquals(1, cache.cachedIds.size)
    assertEquals(1, cache.cachedNames.size)

    // Verify invocations
    verify(client, only()).get(anyGet)

    // Response validation helper
    def responseIsValid = {response: Option[UniqueId] =>
      log.info("Validate response: "+response)
      response match {
        case Some(UniqueId(n, i)) => n == name.toString && i == id
        case _ => false
      }
    }
  }

  @Test
  def testIdMisconfiguredWidth() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val uid = new UIDProvider(marketDbClient, new UIDCache, ByteArray(Table), ByteArray(Kind), 3)

    val id = ByteArray("12")
    val name = "Name"

    val kvs = new ArrayList[KeyValue](1)
    kvs.add(new KeyValue(name, IdFamily, Kind, id))

    when(client.get(any(classOf[GetRequest]))).thenReturn(Deferred.fromResult(kvs))

    intercept[RuntimeException] {
      uid.getId(name).get()
    }
  }

  @Test
  def testIdForNonexistentName() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val provider = new UIDProvider(marketDbClient, new UIDCache, ByteArray(Table), ByteArray(Kind), 3)
    when(client.get(anyGet)).thenReturn(Deferred.fromResult(new ArrayList[KeyValue](0)))

    val noSuchName = "NoSuchName"

    val response = provider.getId(noSuchName).get()
    response match {
      case None => assert(true)
      case _ => assert(false)
    }
  }

  @Test
  def testGetOrCreateIdWithExistingId() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val cache = new UIDCache
    val provider = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)

    val id = ByteArray("123")
    val name = "Name"

    val kvs = new ArrayList[KeyValue](1)
    kvs.add(new KeyValue(name, IdFamily, Kind, id))

    when(client.get(anyGet)).thenReturn(Deferred.fromResult(kvs))

    // First request should be propagated to HBase
    var response = provider.provideId(name).get()
    assert(responseIsValid(response))

    Thread.sleep(10)

    // Second should be a cache hit ...
    response = provider.provideId(name).get()
    assert(responseIsValid(response))

    // Validate cache state
    assertEquals("Cache hits invalid", 1, provider.cacheHits)
    assertEquals("Cache misses invalid", 1+1, provider.cacheMisses)
    assertEquals("Cached ids size invalid", 1, cache.cachedIds.size)
    assertEquals("Cached names size invalid", 1, cache.cachedNames.size)

    // Verify invocations
    verify(client, only()).get(anyGet)

    // Response validation helper
    def responseIsValid = {response: UniqueId =>
      log.info("Validate response: "+response)
      response match {
        case UniqueId(n, i) => n == name && i == id
        case _ => false
      }
    }
  }

  @Test
  def testOrCreateIdAssignIdWithSuccess() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val lock = mock(classOf[RowLock])

    val cache = new UIDCache
    val provider = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)

    val id = ByteArray(Array[Byte](0,0,5))

    // Ackquire lock
    when(client.lockRow(anyRowLockRequest)).thenReturn(Deferred.fromResult(lock))
    // Get returns null
    when(client.get(anyGet))
      .thenAnswer(AnswerWithValue(() => Deferred.fromResult[ArrayList[KeyValue]](null)))
    // Put successfuly
    when(client.put(anyPut)).thenReturn(Deferred.fromResult[AnyRef](null))

    // Update once HBASE-2292 is fixed:
    whenFakeIcvThenReturn(client, 4l)

    // First request should put new id
    var response = provider.provideId("Name").get()
    log.info("First response: " + response)
    assertTrue("Response doesn't match expectable", response match {
      case UniqueId("Name", i) => i == id
      case _ => log.error("Bad response: "+response); false
    })

    // Second response from cache
    response = provider.provideId("Name").get()
    assertTrue("Response doesn't match expectable", response match {
      case UniqueId("Name", i) => i == id
      case _ => log.error("Bad response: "+response); false
    })

    // Third response inverted
    val nameResponse = provider.getName(id).get()
    assertTrue("Response doesn't match expectable", nameResponse match {
      case Some(UniqueId("Name", i)) => i == id
      case _ => log.error("Bad response: "+response); false
    })
    
    // Verify invocations
    verify(client, times(2 + 1)).get(anyGet)
    verify(client).lockRow(anyRowLockRequest)
    verify(client, times(2 + 1)).put(anyPut)
    verify(client).unlockRow(lock)
  }

  @Test
  def testGetOrCreateIdUnableToAcquireRowLock() {
    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)

    val cache = new UIDCache
    val uid = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)
    // 3 Deffered == MaxRetryCount == 3
    when(client.get(anyGet)).thenReturn(Deferred.fromResult[ArrayList[KeyValue]](null))
      .thenReturn(Deferred.fromResult[ArrayList[KeyValue]](null))
      .thenReturn(Deferred.fromResult[ArrayList[KeyValue]](null))

    val hbe = fakeHBaseException
    when(client.lockRow(anyRowLockRequest)).thenThrow(hbe)

    intercept[OopsException] {
      uid.provideId("Name").get()
    }
  }

  @Test
  def testGetOrCreateIdAssignIdWithRaceCondition() {
    // Simulate a race between client A and client B.
    // A does a Get and sees that there's no ID for this name.
    // B does a Get and sees that there's no ID too, and B actually goes
    // through the entire process to create the ID.
    // Then A attempts to go through the process and should discover that the
    // ID has already been assigned.
    
    val clientA = mock(classOf[HBaseClient])
    val marketDbClientA = mock(classOf[Client])
    when(marketDbClientA.apply()).thenReturn(clientA)

    val cacheA = new UIDCache
    val providerA = new UIDProvider(marketDbClientA, cacheA, ByteArray(Table), ByteArray(Kind), 3)

    val clientB = mock(classOf[HBaseClient])
    val marketDbClientB = mock(classOf[Client])
    when(marketDbClientB.apply()).thenReturn(clientB)

    val cacheB = new UIDCache
    val providerB = new UIDProvider(marketDbClientB, cacheB, ByteArray(Table), ByteArray(Kind), 3)

    log.info("CLIENT A: " + clientA)
    log.info("CLIENT B: " + clientB)
    log.info("CACHE A: " + cacheA)
    log.info("CACHE B: " + cacheB)

    val id = ByteArray(Array[Byte](0,0,5))
    val name = "Foo"

    val d = mock(classOf[Deferred[ArrayList[KeyValue]]])
    when(clientA.get(anyGet)).thenReturn(d)

    val theRace: () => ArrayList[KeyValue] = () => {
        // While answering A's first Get, B doest a full getOrCreateId.
        val uid = providerB.provideId(name).get()
        assertTrue(uid match {
          case UniqueId(u, i) => u == name.toString && i == id;
          case _ => false
        })
        null
      }

    val kvs1 = new ArrayList[KeyValue](1)
    kvs1.add(new KeyValue(name, IdFamily, Kind, id))

    // when(d.joinUninterruptibly).thenAnswer(theRace).thenReturn(kvs1)

    when(d.addCallback(any())).thenAnswer(new Answer[Deferred[Unit]] {
      def answer(invocation: InvocationOnMock) = {
        val arguments = invocation.getArguments
        val cb = arguments(0).asInstanceOf[Callback[Unit, ArrayList[KeyValue]]]
        Deferred.fromResult(cb.call(theRace()))
      }
    }).thenAnswer(new Answer[Deferred[Unit]] {
      def answer(invocation: InvocationOnMock) = {
        Thread.sleep(100)
        val arguments = invocation.getArguments
        val cb = arguments(0).asInstanceOf[Callback[Unit, ArrayList[KeyValue]]]
        Deferred.fromResult(cb.call(kvs1))
      }
    })

    val fakeLockA = mock(classOf[RowLock])
    when(clientA.lockRow(anyRowLockRequest)).thenReturn(Deferred.fromResult(fakeLockA))
    when(clientB.get(anyGet)).thenAnswer(AnswerWithValue(() => Deferred.fromResult[ArrayList[KeyValue]](null)))

    val fakeLockB = mock(classOf[RowLock])
    when(clientB.lockRow(anyRowLockRequest)).thenReturn(Deferred.fromResult(fakeLockB))

    val kvs2: ArrayList[KeyValue] = new ArrayList[KeyValue](1)
    kvs2.add(new KeyValue(MaxIdRow, IdFamily, Kind, Bytes.fromLong(4L)))
    when(clientB.get(getForRow(MaxIdRow))).thenReturn(Deferred.fromResult(kvs2))
    when(clientB.put(anyPut)).thenReturn(Deferred.fromResult[AnyRef](null))

    // Get or create Id on provider A
    val uid = providerA.provideId(name).get()
    assertTrue(uid match {
      case UniqueId(u, i) => u == name.toString && i == id;
      case _ => false
    })

    // The +1's below are due to the whenFakeIcvThenReturn() hack.
    // Verify the order of execution too.
    val order = inOrder(clientA, clientB)
    order.verify(clientA).get(anyGet);             // 1st Get for A.
    order.verify(clientB).get(anyGet);           // 1st Get for B.
    order.verify(clientB).lockRow(anyRowLockRequest);  // B starts the process...
    order.verify(clientB, times(1+1)).get(anyGet); // double check for B.
    order.verify(clientB, times(2+1)).put(anyPut); // both mappings.
    order.verify(clientB).unlockRow(fakeLockB);  // ... B finishes.
    order.verify(clientA).lockRow(anyRowLockRequest);  // A starts the process...
    // Finds id cached from B invocation
    order.verify(clientA).unlockRow(fakeLockA);    // ... and stops here.

    // Things A shouldn't do because B did them already:
    verify(clientA, never()).atomicIncrement(any(classOf[AtomicIncrementRequest]))
    verify(clientA, never()).put(anyPut)
  }

  @Test
  // Test the creation of an ID when all possible IDs are already in use
  def testGetOrCreateIdWithOverflow() {
    val cache = new UIDCache

    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val provider = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 1)  // Widht is 1

    val fakeLock = mock(classOf[RowLock])
    when(client.lockRow(anyRowLockRequest)).thenReturn(Deferred.fromResult(fakeLock));

    when(client.get(anyGet))      // null  =>  ID doesn't exist.
      .thenAnswer(AnswerWithValue(() => Deferred.fromResult[ArrayList[KeyValue]](null)))

    // Update once HBASE-2292 is fixed:
    whenFakeIcvThenReturn(client, java.lang.Byte.MAX_VALUE - java.lang.Byte.MIN_VALUE);

    intercept[OopsException] {
      provider.provideId("Foo").get()
    }


    // The +1 below is due to the whenFakeIcvThenReturn() hack.
    verify(client, times((2+1) * 3)).get(anyGet);// Initial Get + double check.
    verify(client, times(3)).lockRow(anyRowLockRequest);      // The .maxid row.
    verify(client, times(3)).unlockRow(fakeLock);     // The .maxid row.
  }

  @Test
  // ICV throws an exception, we can't get an ID.
  def testGetOrCreateIdWithICVFailure() {
    val cache = new UIDCache

    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val provider = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)

    val fakeLock = mock(classOf[RowLock])
    when(client.lockRow(anyRowLockRequest)).thenReturn(Deferred.fromResult(fakeLock));

    when(client.get(anyGet))      // null  =>  ID doesn't exist.
      .thenAnswer(AnswerWithValue(() => Deferred.fromResult[ArrayList[KeyValue]](null)))

    // Update once HBASE-2292 is fixed:
    val kvs = new ArrayList[KeyValue](1);
    kvs.add(new KeyValue(MaxIdRow, IdFamily, Kind, Bytes.fromLong(4L)));

    val hbe = fakeHBaseException
    when(client.get(getForRow(MaxIdRow)))
      .thenThrow(hbe)
      .thenReturn(Deferred.fromResult(kvs))

    when(client.put(anyPut)).thenReturn(Deferred.fromResult[AnyRef](null))

    val id = ByteArray(Array[Byte](0, 0, 5))

    // Get or create id
    val uniqueId = provider.provideId("Foo").get()
    assertTrue("UniqueId doesn't match expectable", uniqueId match {
      case UniqueId("Foo", i) => i == id
      case _ => false
    })

    // The +2/+1 below are due to the whenFakeIcvThenReturn() hack.
    verify(client, times(4+2)).get(anyGet)  // Initial Get + double check x2.
    verify(client, times(2)).lockRow(anyRowLockRequest)   // The .maxid row x2.
    verify(client, times(2+1)).put(anyPut)         // Both mappings.
    verify(client, times(2)).unlockRow(fakeLock)  // The .maxid row x2.
  }

  @Test
  // Test that the reverse mapping is created before the forward one.
  def testGetOrCreateIdPutsReverseMappingFirst() {
    val cache = new UIDCache

    val client = mock(classOf[HBaseClient])
    val marketDbClient = mock(classOf[Client])
    when(marketDbClient.apply()).thenReturn(client)
    val provider = new UIDProvider(marketDbClient, cache, ByteArray(Table), ByteArray(Kind), 3)

    val fakeLock = mock(classOf[RowLock])
    when(client.lockRow(anyRowLockRequest)).thenReturn(Deferred.fromResult(fakeLock));

    when(client.get(anyGet))      // null  =>  ID doesn't exist.
      .thenAnswer(AnswerWithValue(() => Deferred.fromResult[ArrayList[KeyValue]](null)))

    when(client.put(anyPut)).thenReturn(Deferred.fromResult[AnyRef](null))

    // Update once HBASE-2292 is fixed:
    whenFakeIcvThenReturn(client, 5);

    val id = ByteArray(Array[Byte](0, 0, 6))
    val row = ByteArray("foo")

    // Verify returned UniqueId
    val uniqueId = provider.provideId("foo").get()
    assertTrue("UniqueId doesn't match expectable", uniqueId match {
      case UniqueId("foo", i) => i == id
      case _ => false
    })

    val order = inOrder(client)
    order.verify(client).get(anyGet)            // Initial Get.
    order.verify(client).lockRow(anyRowLockRequest)  // The .maxid row.
    // Update once HBASE-2292 is fixed:
    // HACK HACK HACK
    order.verify(client).get(getForRow(row))
    order.verify(client).get(getForRow(MaxIdRow))    // "ICV".
    order.verify(client).put(putForRow(MaxIdRow))    // "ICV".
    // end HACK HACK HACK
    order.verify(client).put(putForRow(id))
    order.verify(client).put(putForRow(row))
    order.verify(client).unlockRow(fakeLock)     // The .maxid row.
  }

  /**Temporary hack until we can do proper ICVs -- see HBASE 2292. */
  private def whenFakeIcvThenReturn(client: HBaseClient, value: Long) = {
    val kvs = new ArrayList[KeyValue](1)
    kvs.add(new KeyValue(MaxIdRow, IdFamily, Kind, Bytes.fromLong(value)))

    val maxIdResult = Deferred.fromResult(kvs)
    when(client.get(getForRow(MaxIdRow))).thenReturn(maxIdResult)
    when(client.put(anyPut)).thenReturn(Deferred.fromResult[AnyRef](null))
  }

  private def anyRowLockRequest = {
    any(classOf[RowLockRequest])
  }

  private def anyGet = {
    any(classOf[GetRequest])
  }

  private def fakeHBaseException = {
    val hbe = mock(classOf[HBaseException])
    when(hbe.getStackTrace).thenReturn(Arrays.copyOf(new RuntimeException().getStackTrace, 3))
    when(hbe.getMessage).thenReturn("fake exception")
    hbe
  }

  private def anyPut = {
    any(classOf[PutRequest])
  }
}