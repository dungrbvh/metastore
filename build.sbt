
logLevel := util.Level.Info

ThisBuild / scalaVersion     := "2.12.14"
ThisBuild / version          := "1.7.6"


lazy val versions = new {
  val jodaConvert = "2.1"
  val jodaTime = "2.9.3"
  val log4j = "1.2.17"
  val mongodb = "3.6.0"
  val scalatest = "3.0.3"
  val spray = "1.3.3"
  val statusUpdater = "0.1.4"
  val twitterLibVersion = "18.6.0"
  val typesafe = "1.3.1"
  val hadoop = "3.2.1"
  val jackson = "2.9.0"
  val gcs = "2.22.0"
}

lazy val root = (project in file("."))
  .configs(Test)
  .settings(
    name := "metastore",
    fork := true,
    parallelExecution := false,
    coverageEnabled := false,
    coverageFailOnMinimum := false,
    scalariformAutoformat := true,
    libraryDependencies ++= Seq(
      "org.mongodb.scala" %% "mongo-scala-driver" % "2.8.0",
      "com.twitter" %% "finagle-http" % versions.twitterLibVersion  exclude("com.twitter", "libthrift"),
      "com.twitter" %% "finagle-serversets" % versions.twitterLibVersion,
      "com.typesafe" % "config" % versions.typesafe,
      "joda-time" % "joda-time" % versions.jodaTime,
      "org.joda" % "joda-convert" % versions.jodaConvert,
      "org.slf4j" % "slf4j-log4j12" % "1.7.30",
      "org.json4s" %% "json4s-native" % "3.2.11",
      "org.scalatest" %% "scalatest" % versions.scalatest % "test",
      "com.github.scopt" %% "scopt" % "3.6.0",
      "org.mockito" %% "mockito-scala" % "1.0.8" %"test",
      "org.apache.hadoop" % "hadoop-client" % versions.hadoop,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % versions.jackson,
      "com.google.cloud" % "google-cloud-storage" % versions.gcs
    )
  )


resolvers ++= Seq(
  ("Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases")
    .withAllowInsecureProtocol(allowInsecureProtocol = true),
  ("SuperConJars" at "https://conjars.wensel.net/repo/")
    .withAllowInsecureProtocol(allowInsecureProtocol = true)
)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

compileScalastyle := (Compile /scalastyle).toTask("").value
(Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}