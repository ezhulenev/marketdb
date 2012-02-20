package com.ergodicity.marketdb.loader

import tools.nsc.io.File

sealed abstract class DataFamily
class PlainText extends DataFamily
class RtsTradeHistory extends DataFamily

sealed abstract class DataRef[F <: DataFamily]
case class Local[F](file: File) extends DataRef
case class Remote[F](url: String) extends DataRef




