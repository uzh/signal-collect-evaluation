import sbt._
import Keys._

object EvalBuild extends Build {
   lazy val scCore = ProjectRef(file("../signal-collect"), id = "signal-collect")
   lazy val scGraphs = ProjectRef(file("../signal-collect-graphs"), id = "signal-collect-graphs")
   val scEval = Project(id = "signal-collect-evaluation",
                         base = file(".")) dependsOn(scGraphs, scCore)
}