package com.ergodicity.marketdb

import scalaz.NonEmptyList

case class Oops(desc: String, cause: Option[Throwable] = None)

class OopsException(errors: NonEmptyList[Oops]) extends RuntimeException {
  override def toString = "Errors caused exception: " + errors
}
