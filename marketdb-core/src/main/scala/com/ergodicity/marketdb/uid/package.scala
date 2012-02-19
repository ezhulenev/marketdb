package com.ergodicity.marketdb

import scalaz._
import Scalaz._

package object uid {
  implicit def nel2list[E](nel: NonEmptyList[E]) = nel.list

  implicit def getBytes(s: String) = s.getBytes
  implicit def ba2array(ba: ByteArray) = ba.toArray

  implicit def ValidationNELPlus[X]: Plus[({type λ[α]=ValidationNEL[X, α]})#λ] = new Plus[({type λ[α]=ValidationNEL[X, α]})#λ] {
    def plus[A](a1: ValidationNEL[X, A], a2: => ValidationNEL[X, A]) = a1 match {
      case Success(_) => a1
      case Failure(f1) => a2 match {
        case Success(_) => a2
        case Failure(f2) => (f1.list <::: f2).fail[A]
      }
    }
  }

}
