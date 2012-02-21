package com.ergodicity.marketdb.loader

import org.joda.time.DateTime
import org.scala_tools.time.Implicits._

case class LoaderSettings(from: DateTime, until: DateTime, resolvers: Seq[DataResolver]) {


}
