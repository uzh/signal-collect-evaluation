import sbt._
import Keys._

object EvalBuild extends Build {
  lazy val scCore = ProjectRef(file("../signal-collect"), id = "signal-collect")
  lazy val yarn = ProjectRef(file("../signal-collect-yarn"), id = "signal-collect-yarn")

  val scEvaluation = Project(id = "signal-collect-evaluation",
    base = file(".")) dependsOn (scCore) dependsOn (yarn)
}
