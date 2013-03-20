package integration.ergodicity.marketdb

import org.slf4j.Logger


trait TimeRecording { self: {def log: Logger} =>

  protected def recordTime[T](prop: String, f: () => T): T = {
    val start = System.currentTimeMillis()
    val result = f()
    val end = System.currentTimeMillis()
    log.info(prop + " time: " + (end - start))
    result
  }
}
