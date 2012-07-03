import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import net.virtualvoid.sbt.graph.Plugin._

object MarketDBBuild extends Build {

  lazy val buildSettings = Seq(
    organization := "com.ergodicity.marketdb",
    version      := "0.1-SNAPSHOT",
    scalaVersion := "2.9.1"
  )

  lazy val marketdb = Project(
    id = "marketdb",
    base = file("."),
    aggregate = Seq(marketdbApi, marketdbCore, marketdbLoader)
  ).configs( IntegrationTest )
    .settings( (Defaults.itSettings ++ graphSettings) : _*)

  lazy val marketdbApi = Project(
    id = "marketdb-api",
    base = file("marketdb-api"),
    settings = Project.defaultSettings ++ repositoriesSetting ++ graphSettings ++ unmanagedSettings ++ scala.Seq[sbt.Project.Setting[_]](
      scalacOptions += "-deprecation",
      libraryDependencies ++= Dependencies.api
    )
  ).configs( IntegrationTest )
    .settings( Defaults.itSettings : _*)

  lazy val marketdbCore = Project(
    id = "marketdb-core",
    base = file("marketdb-core"),
    dependencies = Seq(marketdbApi),
    settings = Project.defaultSettings ++ repositoriesSetting ++ unmanagedSettings ++
      assemblySettings ++ extAssemblySettings ++ graphSettings ++ scala.Seq[sbt.Project.Setting[_]](
      scalacOptions += "-deprecation",
      libraryDependencies ++= Dependencies.core
    )
  ).configs( IntegrationTest )
    .settings( Defaults.itSettings : _*)

  lazy val marketdbLoader = Project(
    id = "marketdb-loader",
    base = file("marketdb-loader"),
    dependencies = Seq(marketdbApi),
    settings = Project.defaultSettings ++ repositoriesSetting ++ unmanagedSettings ++ graphSettings ++ scala.Seq[sbt.Project.Setting[_]](
      scalacOptions += "-deprecation",
      libraryDependencies ++= Dependencies.loader
    )
  ).configs( IntegrationTest )
    .settings( Defaults.itSettings : _*)

  // -- Settings

  override lazy val settings = super.settings ++ buildSettings

  lazy val repositoriesSetting = Seq(
    resolvers += "Sonatype Repository" at "http://oss.sonatype.org/content/groups/public/",
    resolvers += "JBoss repository" at "http://repository.jboss.org/nexus/content/repositories/",
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    resolvers += "Typesafe Repository ide-2.9" at "http://repo.typesafe.com/typesafe/simple/ide-2.9/",
    resolvers += "Twitter Repository" at "http://maven.twttr.com/",
    resolvers += "Akka Repository" at "http://akka.io/snapshots/"
  )

  private val LicenseFile = """(license|licence|notice|copying)([.]\w+)?$""".r
  private def isLicenseFile(fileName: String): Boolean =
    fileName.toLowerCase match {
      case LicenseFile(_, ext) if ext != ".class" => true // DISLIKE
      case _ => false
    }

  private val ReadMe = """(readme)([.]\w+)?$""".r
  private def isReadme(fileName: String): Boolean =
    fileName.toLowerCase match {
      case ReadMe(_, ext) if ext != ".class" => true
      case _ => false
    }

  private object PathList {
    private val sysFileSep = System.getProperty("file.separator")
    def unapplySeq(path: String): Option[List[String]] = {
      val split = path.split(if (sysFileSep.equals( """\""")) """\\""" else sysFileSep)
      if (split.size == 0) None
      else Some(split.toList)
    }
  }

  lazy val extAssemblySettings = scala.Seq[sbt.Project.Setting[_]](
    jarName in assembly <<= (name, version) { (name, version) => "marketdb-" + version + ".jar" } ,
    test in assembly := {},

    mergeStrategy in assembly := {
      case PathList(ps @ _*) if isReadme(ps.last) || isLicenseFile(ps.last) =>
        MergeStrategy.rename
      case PathList("META-INF", xs @ _*) =>
        (xs.map {_.toLowerCase}) match {
          case ("manifest.mf" :: Nil) => MergeStrategy.discard
          case list @ (head :: tail) if (head.last == "manifest.mf") => MergeStrategy.discard
          case list @ (head :: tail) if (head.last == "notice.txt") => MergeStrategy.discard
          case "plexus" :: _ => MergeStrategy.discard
          case "maven" :: _ => MergeStrategy.discard
          case e =>
            System.out.println("EBAKA = "+e)
            MergeStrategy.deduplicate
        }
      case _ => MergeStrategy.deduplicate
    }
  )

  lazy val unmanagedSettings = scala.Seq[sbt.Project.Setting[_]](
    (unmanagedBase <<= baseDirectory { base => base / ".." / "lib" })
  )
}

object Dependencies {
  import Dependency._

  val api = Seq(zeromq, sbinary, jodaTime, jodaConvert, slf4jApi, logback, Test.scalatest, scalaTime)

  val core = Seq(zeromq, ostrich, scalaTime, sbinary, finagleCore, finagleKestrel, scalaSTM, slf4jApi, logback, scalaz, cglib, jodaTime, jodaConvert) ++
    Seq(Test.junit, Test.mockito, Test.powermockApi, Test.powermockJUnit, Test.scalatest, Test.scalacheck, Test.junitInterface) ++
    Seq(asyncHBase, stumbleuponAsync, zookeeper)


  val loader = Seq(zeromq, ostrich, finagleCore, finagleKestrel, scalaIO, httpClient, scalaTime, sbinary, jodaTime, jodaConvert, slf4jApi, logback, scalaz) ++
    Seq(Test.scalatest, Test.scalacheck, Test.mockito)
}


object Dependency {

  // Versions

  object V {
    val Scalatest    = "1.6.1"
    val Slf4j        = "1.6.4"
    val Junit        = "4.5"
    val Mockito      = "1.8.1"
    val Powermock    = "1.4.11"
    val Scalacheck   = "1.9"
    val Scalaz       = "6.0.4"
    val Cglib        = "2.2.2"
    val Logback      = "1.0.0"
    val ScalaSTM     = "0.4"
    val JodaTime     = "2.0"
    val JodaConvert  = "1.2"
    val SBinary      = "0.4.0"
    val ScalaTime    = "0.5"
    val HttpClient   = "3.1"
    val ScalaIO      = "0.3.0"

    // Twitter dependencies
    val Finagle      = "4.0.2"
    val Ostrich      = "7.0.0"

    // Async HBase transisitive hack
    val AsyncHBase              = "1.3.0"
    val StumbleuponAsync        = "1.2.0"
    val Zookeeper               =  "3.4.3"

    // Ergodicity dependencies
    val Zeromq       = "0.1-SNAPSHOT"

  }

  // Compile

  val slf4jApi          = "org.slf4j"                         % "slf4j-api"              % V.Slf4j      // MIT
  val logback           = "ch.qos.logback"                    % "logback-classic"        % V.Logback
  val scalaz            = "org.scalaz"                       %% "scalaz-core"            % V.Scalaz
  val cglib             = "cglib"                             % "cglib"                  % V.Cglib
  val scalaSTM          = "org.scala-tools"                  %% "scala-stm"              % V.ScalaSTM
  val jodaTime          = "joda-time"                         % "joda-time"              % V.JodaTime
  val jodaConvert       = "org.joda"                          % "joda-convert"           % V.JodaConvert
  val finagleCore       = "com.twitter"                      %% "finagle-core"           % V.Finagle
  val finagleKestrel    = "com.twitter"                      %% "finagle-kestrel"        % V.Finagle
  val ostrich           = "com.twitter"                      %% "ostrich"                % V.Ostrich
  val sbinary           = "org.scala-tools.sbinary"          %% "sbinary"                % V.SBinary
  val scalaTime         = "org.scala-tools.time"             %% "time"                   % V.ScalaTime intransitive()
  val httpClient        = "commons-httpclient"                % "commons-httpclient"     % V.HttpClient
  val scalaIO           = "com.github.scala-incubator.io"    %% "scala-io-core"          % V.ScalaIO
  val zeromq            = "com.ergodicity"                   %% "zeromq"                 % V.Zeromq

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
    val junit          = "junit"                       % "junit"                   % V.Junit        % "test" // Common Public License 1.0
    val mockito        = "org.mockito"                 % "mockito-all"             % V.Mockito      % "test" // MIT
    val powermockApi   = "org.powermock"               % "powermock-api-mockito"   % V.Powermock    % "test"
    val powermockJUnit = "org.powermock"               % "powermock-module-junit4" % V.Powermock    % "test"
    val scalatest      = "org.scalatest"              %% "scalatest"               % V.Scalatest    % "it,test" // ApacheV2
    val scalacheck     = "org.scala-tools.testing"    %% "scalacheck"              % V.Scalacheck   % "test" // New BSD
    val junitInterface = "com.novocode"                % "junit-interface"         % "0.8"          % "test"
  }
}