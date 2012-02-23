package com.ergodicity.marketdb.loader

import java.io.{File, InputStream}


sealed abstract class DataRef
case class InputStreamRef(is: InputStream) extends DataRef
case class LocalRef(file: File) extends DataRef
case class RemoteRef(url: String) extends DataRef

sealed trait TradeData[R <: DataRef] {
  val ref: R
}
case class RtsTradeHistory[R <: DataRef](ref: R) extends TradeData[R]







