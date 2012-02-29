package com.ergodicity.marketdb

import org.mockito.Matchers._
import org.hamcrest.{Description, BaseMatcher}
import java.util.Arrays
import java.lang.reflect.Field
import org.hbase.async.{GetRequest, HBaseRpc, PutRequest}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

trait HBaseMatchers {

  protected def getForRow(row: ByteArray): GetRequest = {
    argThat(new BaseMatcher[GetRequest] {
      def describeTo(description: Description) {
        description.appendText("GetRequest for row " + row.toString())
      }

      def matches(item: AnyRef) = Arrays.equals(extractKey(item.asInstanceOf[GetRequest]), row.toArray)
    })
  }

  protected def putForRow(row: ByteArray): PutRequest = {
    argThat(new BaseMatcher[PutRequest] {
      def describeTo(description: Description) {
        description.appendText("PutRequest for row " + row)
      }

      def matches(item: AnyRef) = Arrays.equals(extractKey(item.asInstanceOf[PutRequest]), row.toArray)
    })
  }

  protected def extractKey(rpc: HBaseRpc): Array[Byte] = {
    try {
      val key: Field = classOf[HBaseRpc].getDeclaredField("key")
      key.setAccessible(true)
      key.get(rpc).asInstanceOf[Array[Byte]]
    } catch {
      case e: Exception => throw new RuntimeException("failed to extract the key out of " + rpc, e)
    }
  }

  protected def AnswerWithValue[A](v: () => A) = new Answer[A] {
    def answer(invocation: InvocationOnMock) = v()
  }

}
