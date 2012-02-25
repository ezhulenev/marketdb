package com.ergodicity.marketdb.loader

import org.joda.time.LocalDate
import java.io.File
import org.joda.time.format.DateTimeFormat
import org.apache.commons.httpclient.{HttpStatus, HttpClient}
import org.slf4j.LoggerFactory
import org.apache.commons.httpclient.methods.{HeadMethod, GetMethod}

class TradeResolver[R <: DataRef, D <: TradeData[R]](ref: RefResolver[R], builder: R => D) {
  def resolve(day: LocalDate): Option[D] = ref.resolve(day) map {
    ref => builder(ref)
  }
}

trait RefResolver[R <: DataRef] {
  def resolve(day: LocalDate): Option[R]
}

object RefResolver {
  val log = LoggerFactory.getLogger("RefResolver")

  def apply(dir: File, pattern: String): RefResolver[LocalRef] = new RefResolver[LocalRef] {
    if (!dir.isDirectory) {
      throw new IllegalArgumentException("Directory doesn't exists: " + dir)
    }

    val Formatter = DateTimeFormat.forPattern(pattern)

    def resolve(day: LocalDate) = {
      val file = new File(dir, Formatter.print(day))

      if (file.isFile)
        Some(LocalRef(file))
      else
        None
    }
  }

  def apply(baseUrl: String, pattern: String)(implicit client: HttpClient): RefResolver[RemoteRef] = new RefResolver[RemoteRef] {
    val Formatter = DateTimeFormat.forPattern(pattern)

    def resolve(day: LocalDate): Option[RemoteRef] = {
      val url: String = baseUrl + "/" + Formatter.print(day)
      val get = new HeadMethod(url)
      get.getParams
      try {
        val responseCode = client.executeMethod(get)
        if (responseCode != HttpStatus.SC_OK) None else Some(RemoteRef(url))
      } catch {
        case e => log.error("Error resolving remore ref: " + e); None
      } finally {
        get.releaseConnection()
      }
    }
  }
}