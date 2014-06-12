name := "scalding-taps"

organization := "io.scalding"

version := "0.6_scalding0.10"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
    "org.elasticsearch" % "elasticsearch-hadoop" % "2.0.0",
    "com.ebay" % "cascading-hive" % "0.0.2-SNAPSHOT",
    "com.twitter" %% "scalding-core" % "0.10.0",
    "org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.3.1",
    "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.3.1",
    "cascading" % "cascading-core" % "2.5.4",
    "cascading" % "cascading-platform" % "2.5.4",
    "cascading" % "cascading-hadoop" % "2.5.4",
    "cascading" % "cascading-local" % "2.5.4",
    "com.github.tlrx" % "elasticsearch-test" % "1.1.0" % "test",
    "org.scalatest" %% "scalatest" % "2.1.0" % "test"
)

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "Conjars" at "http://conjars.org/repo",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)
