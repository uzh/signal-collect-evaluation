import sbt._
import Keys._

object EvalBuild extends Build {
  lazy val scCore = ProjectRef(file("../signal-collect"), id = "signal-collect")
  val scEvaluation = Project(id = "signal-collect-evaluation",
    base = file(".")) dependsOn (scCore)
}
