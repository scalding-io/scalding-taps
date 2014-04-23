name := "scalding-taps"

organization := "io.scalding"

version := "0.3"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
    "org.elasticsearch" % "elasticsearch-hadoop" % "1.3.0.M3",
    "com.ebay" % "cascading-hive" % "0.0.1-SNAPSHOT",
    "com.twitter" %% "scalding-core" % "0.8.11",
    "org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.3.1",
    "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.3.1",
    "cascading" % "cascading-core" % "2.5.2",
    "cascading" % "cascading-platform" % "2.5.2",
    "cascading" % "cascading-hadoop" % "2.5.2",
    "cascading" % "cascading-local" % "2.5.2",
    "com.github.tlrx" % "elasticsearch-test" % "0.0.8" % "test",
    "org.scalatest" %% "scalatest" % "2.1.0" % "test"
)

resolvers ++= Seq(
  "Conjars" at "http://conjars.org/repo",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)
