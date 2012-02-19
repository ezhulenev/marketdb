package com.ergodicity.marketdb

import com.stumbleupon.async.Callback

object AsyncHBase {
  implicit def f2callback[R, T](f: T => R) = new Callback[R, T] {
    def call(p: T) = f(p)
  }
}
