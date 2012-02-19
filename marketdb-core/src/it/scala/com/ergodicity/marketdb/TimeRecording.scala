package com.ergodicity.marketdb

import org.slf4j.LoggerFactory

trait TimeRecording {
  val log = LoggerFactory.getLogger(classOf[TimeRecording])

  protected def recordTime[T](prop: String, f: () => T): T = {
    val start = System.currentTimeMillis()
    val result = f()
    val end = System.currentTimeMillis()
    log.info(prop + " time: " + (end - start))
    result
  }
}
