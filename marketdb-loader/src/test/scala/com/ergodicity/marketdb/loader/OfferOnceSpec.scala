package com.ergodicity.marketdb.loader

import org.scalatest.WordSpec
import util.OfferOnce
import org.slf4j.LoggerFactory


class OfferOnceSpec extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[OfferOnceSpec])

  "OfferOnce" must {
    "offer data only once" in {

      val message = "Offer"
      val offerOnce = OfferOnce(message)

      var cnt = 0;

      offerOnce.foreach(msg => {
        log.info("Offered message = " + msg)
        cnt = cnt + 1
      })

      assert(cnt == 1)
    }
  }
}