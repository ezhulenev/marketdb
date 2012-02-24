package com.ergodicity.marketdb.loader.util

import org.scala_tools.time.Implicits._
import org.joda.time.{Interval, LocalDate}

object Implicits {
  implicit def wrapLocalDate(date: LocalDate) = new LocalDateW(date)

  implicit def wrapInterval(interval: Interval) = new IntervalW(interval)
}

object JodaTimeUtils {
  def createLocalDateList(from: LocalDate, to: LocalDate): List[LocalDate] = {
    if (from <= to)
      from :: createLocalDateList(from + 1.day, to)
    else
      List()
  }
}

class IntervalW(underlying: Interval) {

  import JodaTimeUtils._

  def toDays: List[LocalDate] = createLocalDateList(underlying.start.toLocalDate, underlying.end.toLocalDate)
}

class LocalDateW(underlying: LocalDate) {

  import JodaTimeUtils._

  def to(other: LocalDate): List[LocalDate] = {
    if (other < underlying) {
      throw new IllegalArgumentException("The end instant must be greater or equal to the start")
    }
    createLocalDateList(underlying, other)
  }
}