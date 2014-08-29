import sbt._

object Dependency {

  // Versions

  object V {
    val Scalatest    = "1.8"
    val Slf4j        = "1.6.4"
    val Junit        = "4.5"
    val Mockito      = "1.9.0"
    val Powermock    = "1.4.11"
    val Scalaz       = "6.0.4"
    val Cglib        = "2.2.2"
    val Logback      = "1.0.0"
    val ScalaSTM     = "0.6"
    val JodaTime     = "2.0"
    val JodaConvert  = "1.2"
    val SBinary      = "0.4.0"
    val ScalaTime    = "0.5"
    val HttpClient   = "3.1"
    val ScalaIO      = "0.4.1"

    // Twitter dependencies
    val Finagle      = "5.3.6"
    val Ostrich      = "8.2.3"

    // Async HBase transisitive hack
    val AsyncHBase              = "1.3.2"
    val StumbleuponAsync        = "1.2.0"
    val Zookeeper               = "3.4.3"
  }

  // Compile

  val slf4jApi          = "org.slf4j"                         % "slf4j-api"              % V.Slf4j
  val logback           = "ch.qos.logback"                    % "logback-classic"        % V.Logback
  val scalaz            = "org.scalaz"                       %% "scalaz-core"            % V.Scalaz
  val cglib             = "cglib"                             % "cglib"                  % V.Cglib
  val scalaSTM          = "org.scala-tools"                  %% "scala-stm"              % V.ScalaSTM
  val jodaTime          = "joda-time"                         % "joda-time"              % V.JodaTime
  val jodaConvert       = "org.joda"                          % "joda-convert"           % V.JodaConvert
  val finagleCore       = "com.twitter"                       % "finagle-core"           % V.Finagle
  val finagleKestrel    = "com.twitter"                       % "finagle-kestrel"        % V.Finagle
  val ostrich           = "com.twitter"                       % "ostrich"                % V.Ostrich
  val sbinary           = "org.scala-tools.sbinary"          %% "sbinary"                % V.SBinary
  val scalaTime         = "org.scala-tools.time"             %% "time"                   % V.ScalaTime intransitive()
  val httpClient        = "commons-httpclient"                % "commons-httpclient"     % V.HttpClient
  val scalaIO           = "com.github.scala-incubator.io"    %% "scala-io-core"          % V.ScalaIO
  val mockito           = "org.mockito"                       % "mockito-all"            % V.Mockito

  val asyncHBase        = "org.hbase"                        % "asynchbase"              % V.AsyncHBase intransitive()
  val stumbleuponAsync  = "com.stumbleupon"                   % "async"                  % V.StumbleuponAsync intransitive()
  val zookeeper         = "org.apache.zookeeper"              % "zookeeper"              % V.Zookeeper intransitive()

  // Provided

  object Provided {

  }

  // Runtime

  object Runtime {

  }

  // Test

  object Test {
    val junit          = "junit"                       % "junit"                   % V.Junit        % "test"
    val mockito        = "org.mockito"                 % "mockito-all"             % V.Mockito      % "test"
    val powermockApi   = "org.powermock"               % "powermock-api-mockito"   % V.Powermock    % "test"
    val powermockJUnit = "org.powermock"               % "powermock-module-junit4" % V.Powermock    % "test"
    val scalatest      = "org.scalatest"              %% "scalatest"               % V.Scalatest    % "test"
    val junitInterface = "com.novocode"                % "junit-interface"         % "0.8"          % "test"
  }
}