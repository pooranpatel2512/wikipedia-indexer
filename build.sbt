
lazy val commonSettings = Seq(
  organization := "com.pooranpatel",
  version := "0.1.0",
  scalaVersion := "2.11.6"
)

lazy val root = (project in file(".")).settings(
  name := "WikipediaIndexer",
  libraryDependencies := Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.3.9",
    "org.apache.lucene" % "lucene-core" % "5.1.0",
    "org.apache.lucene" % "lucene-analyzers-common" % "5.1.0",
    "joda-time" % "joda-time" % "2.7"
  )
)