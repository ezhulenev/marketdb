import bintray.Keys._

organization in ThisBuild := "com.scalafi.marketdb"

version in ThisBuild := "0.0.1"

scalaVersion in ThisBuild := "2.9.1"

scalacOptions in ThisBuild ++= Seq("-deprecation", "-unchecked")

maxErrors in ThisBuild := 5

resolvers in ThisBuild ++= Seq(
  "Sonatype Repository"         at "http://oss.sonatype.org/content/groups/public/",
  "Scala Tools Repository"      at "https://oss.sonatype.org/content/groups/scala-tools/",
  "JBoss repository"            at "http://repository.jboss.org/nexus/content/repositories/",
  "Typesafe Repository"         at "http://repo.typesafe.com/typesafe/releases/",
  "Typesafe Repository ide-2.9" at "http://repo.typesafe.com/typesafe/simple/ide-2.9/",
  "Twitter Repository"          at "http://maven.twttr.com/",
  "Akka Repository"             at "http://akka.io/snapshots/"
)

licenses in ThisBuild += ("MIT", url("http://opensource.org/licenses/MIT"))

// Projects

def MarketDbProject(name: String) = {
  Project(id = name, base = file(name)).
    settings(bintrayPublishSettings:_*).
    settings(repository in bintray := "releases").
    settings(bintrayOrganization in bintray := None)
}

lazy val root = Project(id = "marketdb", base = file(".")).
  settings(publish :=()).
  settings(publishLocal :=()).
  aggregate(marketdbApi, marketdbApp, marketdbCore, marketdbIteratee, marketdbLoader)

lazy val marketdbApi = MarketDbProject("marketdb-api")

lazy val marketdbApp = MarketDbProject("marketdb-app").
    dependsOn(marketdbCore, marketdbIteratee)

lazy val marketdbCore = MarketDbProject("marketdb-core").
  dependsOn(marketdbApi)

lazy val marketdbIteratee = MarketDbProject("marketdb-iteratee").
  dependsOn(marketdbApi, marketdbCore % "test->test")

lazy val marketdbLoader = MarketDbProject("marketdb-loader").
  dependsOn(marketdbApi)