import sbt._


object Dependencies {
  val sparkVersion = "2.4.1"
  val scoptVersion = "3.7.0"
  val akkaHttpVersion = "10.1.5"
  val circeVersion = "0.10.0"
  val scalatestVersion = "3.0.5"
  val akkaVersion = "2.5.12"

  lazy val deps =
    Seq[ModuleID](
      "org.apache.spark"      %% "spark-core"                 % sparkVersion,
      "org.apache.spark"      %% "spark-sql"                  % sparkVersion,
      "org.apache.spark"      %% "spark-mllib"                % sparkVersion,
      "org.apache.spark"      %% "spark-streaming"            % sparkVersion,
      "com.github.scopt"      %% "scopt"                      % scoptVersion,
      "com.typesafe.akka"     %% "akka-http"                  % akkaHttpVersion,
      "com.typesafe.akka"     %% "akka-actor"                 % akkaVersion,
      "com.typesafe.akka"     %% "akka-stream"                % akkaVersion,
      "io.circe"              %% "circe-core"                 % circeVersion,
      "io.circe"              %% "circe-generic"              % circeVersion,
      "io.circe"              %% "circe-parser"               % circeVersion,

      "org.scalatest"         %% "scalatest"                  % scalatestVersion       % Test
    )
}
