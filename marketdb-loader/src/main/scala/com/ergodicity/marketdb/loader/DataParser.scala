package com.ergodicity.marketdb.loader

trait DataParser[F] {
  def read: String
}

object DataParser {
  implicit  def PlainTextReader: DataParser[PlainText] = new DataParser[PlainText] {
    def read = "READ PLAIN TEXT"
  }

  implicit def RtsTradeHistoryReader: DataParser[RtsTradeHistory] = new DataParser[RtsTradeHistory] {
    def read = "READ RTS HISTORY"
  }
}