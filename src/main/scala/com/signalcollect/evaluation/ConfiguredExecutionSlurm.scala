package com.signalcollect.evaluation

import com.signalcollect.deployment.SlurmExecution

object ConfiguredExecutionSlurm extends App {
  SlurmExecution.deployToSlurm(args)
}
