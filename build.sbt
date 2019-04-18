import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.mrka"
ThisBuild / organizationName := "mrka"


resolvers ++= Seq(
  "apache-snapshots" at "http://central.maven.org/maven2/"
)

lazy val root = (project in file("."))
  .settings(
    name := "sparkGames",
    libraryDependencies ++= deps
  )

