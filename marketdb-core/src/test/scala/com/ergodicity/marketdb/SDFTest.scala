package com.ergodicity.marketdb

import scalaz._
import Scalaz._
import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import java.util.Random
import collection.mutable.Stack
import com.twitter.util.{Promise, Future}
import com.twitter.concurrent.Offer
import java.util.concurrent.atomic.AtomicReference
import org.junit.{Ignore, Test}


@Ignore
class SDFTest {
  val log = LoggerFactory.getLogger(classOf[SDFTest])

  val sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")

  @Test
  def test() {
    val validation: Validation[String, Option[Int]] = Some(100).success[String]

    val validatedTwice: Validation[String, Option[Int]] = validation.fold(
      _ => validation, // if Failure then return it
      _.map(validateValue _).map(_.map(Some(_))) getOrElse validation
    )

    val validatedTwice2: Validation[String, Option[Int]] = validation.flatMap(
      _.map(validateValue _).map(_.map(Some(_))) getOrElse validation
    )

    val vv = validation flatMap (_.filter(_ == 100).toSuccess("Bad value found"))

    log.info("validatedTwice: " + validatedTwice)
    log.info("validatedTwice2: " + validatedTwice2)
    log.info("vv: " + vv)

  }

  def validateValue(value: Int): Validation[String, Int] = {
    if (value == 100)
      value.success[String]
    else
      "Bad value".fail[Int]
  }

  @Test
  def testLazyExecution() {

    val retried = retryUntilValid(10) {
      internalHeavyComputation _
    }

    println(retried)

    val start = System.currentTimeMillis()
    val retried2 = retryUntilValid2(10) {
      internalHeavyComputation2 _
    }
    val end = System.currentTimeMillis()
    log.info("Retry2: " + (end - start))

    val retried2val = retried2()
    val end2 = System.currentTimeMillis()
    log.info("Retry2Val: " + (end2 - end))

    println(retried2val)

  }

  def retryUntilValid[E, A](retryCount: Int)
                           (computation: () => ValidationNEL[E, A]): ValidationNEL[E, A] = {
    val errSuccessStreams = Stream.iterate(computation())(prev => {
      val curr = computation()
      prev <+> curr
    }) take retryCount span (_.isFailure)
    (errSuccessStreams._2 ++ errSuccessStreams._1.reverse) head
  }

  val stack = Stack(10, 10, 10, 95, 96, 97);

  private def internalHeavyComputation(): ValidationNEL[String, Int] = {
    val rand = new Random()
    val v = rand.nextInt(100)
    log.info("Start inner computation; Value = " + v)
    if (v > 80)
      v.successNel[String]
    else
      ("Failed:" + v).failNel[Int]
  }

  def retryUntilValid2[E, A](retryCount: Int)
                            (computation: () => Future[ValidationNEL[E, A]]): Future[ValidationNEL[E, A]] = {

    val promise = new Promise[ValidationNEL[E, A]]

    val computationResult = computation()
    computationResult onSuccess {
      value =>
        promise.setValue(value)
    }

    var counter = 0;
    def retryComputation(v: ValidationNEL[E, A]): Future[ValidationNEL[E, A]] = {
      if (v.isSuccess)
        Future(v)
      else if (counter < retryCount) {
        counter = counter + 1;
        computation().flatMap(r => retryComputation(v <+> r))
      } else Future(v)
    }

    promise
  }

  private def internalHeavyComputation2(): Future[ValidationNEL[String, Int]] = {
    val rand = new Random()
    val v = rand.nextInt(100)
    log.info("Start inner computation; Value = " + v)
    if (v > 80)
      Future {
        Thread.sleep(v * 10); v.successNel[String]
      }
    else
      Future {
        Thread.sleep(v * 10); ("Failed:" + v).failNel[Int]
      }
  }


}