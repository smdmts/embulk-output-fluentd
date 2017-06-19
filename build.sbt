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


lazy val circeVersion = "0.8.0"
libraryDependencies ++= Seq(
  "org.jruby" % "jruby-complete" % "1.6.5",
  "org.embulk" % "embulk-core" % "0.8.24",
  "org.komamitsu" % "fluency" % "1.1.0",
  "org.scalacheck" %% "scalacheck" % "1.13.4"  % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.5" % Test
)