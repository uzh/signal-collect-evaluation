import sbt._
import Keys._

object EvalBuild extends Build {
  lazy val scCore = ProjectRef(file("../signal-collect"), id = "signal-collect")
  lazy val scGraphs = ProjectRef(file("../signal-collect-graphs"), id = "signal-collect-graphs")
  lazy val slurmDeployment = ProjectRef(file("../signal-collect-slurm"), id = "signal-collect-slurm")
  val scEvaluation = Project(id = "signal-collect-evaluation",
    base = file(".")) dependsOn (scCore) dependsOn (scGraphs) dependsOn (slurmDeployment)
}
