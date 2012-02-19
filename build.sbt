
resolvers += "Sonatype Repository" at "http://oss.sonatype.org/content/groups/public/"

resolvers += "JBoss repository" at "http://repository.jboss.org/nexus/content/repositories/"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Typesafe Repository ide-2.9" at "http://repo.typesafe.com/typesafe/simple/ide-2.9/"

resolvers += "Twitter Repository" at "http://maven.twttr.com/"

resolvers += "Akka Repository" at "http://akka.io/snapshots/"

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")