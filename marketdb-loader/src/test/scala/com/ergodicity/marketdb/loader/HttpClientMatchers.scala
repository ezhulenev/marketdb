package com.ergodicity.marketdb.loader

import org.apache.commons.httpclient.methods.GetMethod
import org.mockito.Matchers._
import org.hamcrest.{Description, BaseMatcher}
import java.io.InputStream
import org.apache.commons.httpclient.HttpMethodBase

trait HttpClientMatchers {
  def getMethodFor(url: String): GetMethod = argThat(new BaseMatcher[GetMethod] {
    def describeTo(description: Description) {
      "Get method for url: " + url
    }

    def matches(item: AnyRef) = {
      item.isInstanceOf[GetMethod] && {
        val get = item.asInstanceOf[GetMethod]
        get.getURI.toString == url
      }
    }
  })

  def getMethodFor(url: String, is: =>InputStream): GetMethod = argThat(new BaseMatcher[GetMethod] {
    def describeTo(description: Description) {
      "Get method for url: " + url
    }

    def matches(item: AnyRef) = {
      item.isInstanceOf[GetMethod] && {
        val get = item.asInstanceOf[GetMethod]
        val matches = get.getURI.toString == url
        if (matches) {
          val field = classOf[HttpMethodBase].getDeclaredField("responseStream")
          field.setAccessible(true)
          field.set(get, is)
        }
        matches
      }
    }
  })

}
