enablePlugins(ScalafmtPlugin)

lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.example",
      scalaVersion := "2.12.6",
      version := "0.1.0-SNAPSHOT"
    )),
  name := "embulk-output-fluentd",
  scalafmtOnCompile in ThisBuild := true,
  scalafmtTestOnCompile in ThisBuild := true
)

resolvers += Resolver.jcenterRepo
resolvers += Resolver.sonatypeRepo("releases")
resolvers += "velvia maven" at "http://dl.bintray.com/velvia/maven"

libraryDependencies ++= Seq(
  "org.jruby"                  % "jruby-complete"               % "1.6.5",
  "org.embulk"                 % "embulk-core"                  % "0.9.7",
  "com.typesafe.akka"          %% "akka-actor"                  % "2.5.16",
  "com.typesafe.akka"          %% "akka-stream"                 % "2.5.16",
  "com.typesafe.akka"          %% "akka-slf4j"                  % "2.5.16",
  "org.velvia"                 %% "msgpack4s"                   % "0.6.0",
  "org.wvlet.airframe"         %% "airframe"                    % "0.65",
  "io.monix"                   %% "monix"                       % "3.0.0-RC1",
  "com.typesafe.akka"          %% "akka-stream-testkit"         % "2.5.16" % Test,
  "org.scalacheck"             %% "scalacheck"                  % "1.13.4" % Test,
  "org.scalatest"              %% "scalatest"                   % "3.0.1" % Test,
  "org.scalamock"              %% "scalamock-scalatest-support" % "3.6.0" % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13"   % "1.1.5" % Test
)
