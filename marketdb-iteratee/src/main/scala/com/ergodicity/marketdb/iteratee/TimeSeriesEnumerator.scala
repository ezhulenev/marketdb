package com.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.TimeSeries
import com.ergodicity.marketdb.model.MarketPayload
import scalaz.NonEmptyList

object TimeSeriesEnumerator {
  def apply[P <: MarketPayload](timeSeries: TimeSeries[P]) = new SingleTimeSeriesEnumerator[P](timeSeries)

  def apply[P <: MarketPayload](timeSeries: NonEmptyList[TimeSeries[P]]) = new MultipleTimeSeriesEnumerator[P](timeSeries)

}
