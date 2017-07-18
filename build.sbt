lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.11",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "embulk-output-fluentd"
  )

enablePlugins(ScalafmtPlugin)

resolvers += Resolver.jcenterRepo
resolvers += Resolver.sonatypeRepo("releases")
resolvers += "velvia maven" at "http://dl.bintray.com/velvia/maven"

libraryDependencies ++= Seq(
  "org.jruby" % "jruby-complete" % "1.6.5",
  "org.embulk" % "embulk-core" % "0.8.25",
  "com.typesafe.akka" %% "akka-actor" % "2.5.3",
  "com.typesafe.akka" %% "akka-stream" % "2.5.3",
  "com.typesafe.akka" %% "akka-slf4j" % "2.5.3",
  "org.velvia" %% "msgpack4s" % "0.6.0",
  "org.wvlet" %% "airframe" % "0.15",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.3" % Test,
  "org.scalacheck" %% "scalacheck" % "1.13.4"  % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.5" % Test
)