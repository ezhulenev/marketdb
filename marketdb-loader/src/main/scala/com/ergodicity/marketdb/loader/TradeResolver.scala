package com.ergodicity.marketdb.loader

import org.joda.time.LocalDate
import java.io.File
import org.joda.time.format.DateTimeFormat

class TradeResolver[R <: DataRef, D <: TradeData[R]](ref: RefResolver[R], builder: R => D) {
  def resolve(day: LocalDate): Option[D] = ref.resolve(day) map {
    ref => builder(ref)
  }
}

trait RefResolver[R <: DataRef] {
  def resolve(day: LocalDate): Option[R]
}

object RefResolver {
  def apply(dir: File, pattern: String): RefResolver[LocalRef] = new RefResolver[LocalRef] {
    if (!dir.isDirectory) {
      throw new IllegalArgumentException("Directory doesn't exists: " + dir)
    }

    val Formatter = DateTimeFormat.forPattern(pattern);

    def resolve(day: LocalDate) = {
      val file = new File(dir, Formatter.print(day))

      if (file.isFile)
        Some(LocalRef(file))
      else
        None
    }
  }
}