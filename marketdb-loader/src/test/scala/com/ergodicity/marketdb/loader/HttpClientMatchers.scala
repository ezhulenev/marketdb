package com.ergodicity.marketdb.loader

import org.mockito.Matchers._
import org.hamcrest.{Description, BaseMatcher}
import java.io.InputStream
import org.apache.commons.httpclient.HttpMethodBase
import org.apache.commons.httpclient.methods.{HeadMethod, GetMethod}

trait HttpClientMatchers {
  def headMethodFor(url: String): HeadMethod = argThat(new BaseMatcher[HeadMethod] {
    def describeTo(description: Description) {
      "Get method for url: " + url
    }

    def matches(item: AnyRef) = {
      item.isInstanceOf[HeadMethod] && {
        val get = item.asInstanceOf[HeadMethod]
        get.getURI.toString == url
      }
    }
  })


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
