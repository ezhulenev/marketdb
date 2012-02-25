package com.ergodicity.marketdb.loader

import org.slf4j.LoggerFactory
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import org.apache.commons.cli._
import org.scala_tools.time.Implicits._
import org.joda.time.Interval
import com.ergodicity.marketdb.loader.util.Implicits._
import util.Iteratees

object App {
  val Format = DateTimeFormat.forPattern("yyyyMMdd")

  def main(args: Array[String]) {

    val options = AppOptions.buildOptions()
    val parser = new PosixParser();
    try {
      val cmd = parser.parse(options, args);

      val loader = cmd.getOptionValue(AppOptions.LOADER_OPT)
      val from = Format.parseDateTime(cmd.getOptionValue(AppOptions.FROM_OPT));
      val until = Format.parseDateTime(cmd.getOptionValue(AppOptions.TO_OPT));

      new App(loader, from to until);
    } catch {
      case e: ParseException => {
        System.err.println("Parsing failed.  Reason: " + e.getMessage);
        val formatter = new HelpFormatter();
        formatter.printHelp("marketDB loader", options);
      }
    }
  }
}

class App(loader: String, interval: Interval) {
  val log = LoggerFactory.getLogger(classOf[App])

  log.info("Starting marketDB loader")
  log.info("Loader: " + loader)
  log.info("Date interval: " + interval)

  val context = new ClassPathXmlApplicationContext("applicationContext.xml")
  (context.asInstanceOf[ConfigurableApplicationContext]).registerShutdownHook()

  val loaderBean = context.getBean(loader, classOf[TradeLoader])
  if (loaderBean == null) {
    throw new IllegalArgumentException("Can't find loader by name: " + loader)
  }

  import Iteratees._

  for (day <- interval.toDays) {
    log.info("Load data for: " + day)
    val count = loaderBean.enumTrades(day, counter)
    log.info("Loader report for day: " + day + "; Report: " + count)
  }

  log.info("Shutdown marketDB loader")
}
