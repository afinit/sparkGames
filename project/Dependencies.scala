import sbt._


object Dependencies {
  val sparkVersion = "2.4.1"
  val scoptVersion = "3.7.0"

  lazy val deps = Def.setting(
    Seq[ModuleID](
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
  )
}
