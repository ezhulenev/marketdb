package com.ergodicity.marketdb.loader

import org.joda.time.DateTime

case class LoaderSettings(from: DateTime, until: DateTime, resolvers: Seq[DataResolver]) {


}
