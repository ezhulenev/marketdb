object Dependencies {
  import Dependency._

  val api = Seq(finagleCore, sbinary, jodaTime, jodaConvert, slf4jApi, logback, scalaTime) ++ Seq(Test.scalatest, Test.mockito)

  val app = Seq() ++ Seq(Test.junit, Test.mockito, Test.powermockApi, Test.powermockJUnit, Test.scalatest, Test.junitInterface)

  val core = Seq(ostrich, scalaTime, sbinary, finagleCore, finagleKestrel, scalaSTM, slf4jApi, logback, scalaz, cglib, jodaTime, jodaConvert) ++
    Seq(asyncHBase, stumbleuponAsync, zookeeper) ++
    Seq(Test.junit, Test.mockito, Test.powermockApi, Test.powermockJUnit, Test.scalatest, Test.junitInterface)

  val iteratee = Seq(scalaz) ++ Seq(asyncHBase, stumbleuponAsync, zookeeper) ++
    Seq(Test.junit, Test.mockito, Test.powermockApi, Test.powermockJUnit, Test.scalatest, Test.junitInterface)

  val loader = Seq(ostrich, finagleCore, finagleKestrel, scalaIO, httpClient, scalaTime, sbinary, jodaTime, jodaConvert, slf4jApi, logback, scalaz) ++
    Seq(Test.scalatest, Test.mockito)
}