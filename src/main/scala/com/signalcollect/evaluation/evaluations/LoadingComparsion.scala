package com.signalcollect.evaluation.evaluations

import com.signalcollect.evaluation.jobsubmission._
import com.signalcollect.nodeprovisioning.torque._
import com.signalcollect.evaluation.algorithms._
import com.signalcollect.configuration._
import com.signalcollect.evaluation.resulthandling._
import com.signalcollect.graphproviders.synthetic._
import com.signalcollect._

object LoadingComparsion extends App {

  val evaluation = new EvaluationSuiteCreator(evaluationName = "Loading Evaluation",
  executionHost = new TorqueHost(torqueHostname = "kraken.ifi.uzh.ch", localJarPath = "./target/signal-collect-evaluation-2.0.0-SNAPSHOT-jar-with-dependencies.jar", torqueUsername = System.getProperty("user.name")))
  
  val centralizedLoading = new PageRankEvaluationRun(executionConfiguration = ExecutionConfiguration(ExecutionMode.OptimizedAsynchronous), graphProvider = new LogNormalGraph(graphSize = 200000))
  
  val distributedLoading = new PageRankEvaluationRun(executionConfiguration = ExecutionConfiguration(ExecutionMode.OptimizedAsynchronous), graphProvider = new DistributedLogNormal(graphSize = 200000, numberOfWorkers = Some(24)))
  
  val distributedRandomly = new PageRankEvaluationRun(executionConfiguration = ExecutionConfiguration(ExecutionMode.OptimizedAsynchronous), graphProvider = new DistributedLogNormal(graphSize = 200000))
  
  for (i <- 0 until 2) {
    evaluation.addJobForEvaluationAlgorithm(centralizedLoading)

    evaluation.addJobForEvaluationAlgorithm(distributedLoading)

    evaluation.addJobForEvaluationAlgorithm(distributedRandomly)

  }
  
  
  evaluation.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  evaluation.runEvaluation()
}