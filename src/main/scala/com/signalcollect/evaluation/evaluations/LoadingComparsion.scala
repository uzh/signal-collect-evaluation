package com.signalcollect.evaluation.evaluations

import com.signalcollect.evaluation.jobsubmission._
import com.signalcollect.evaluation.jobexecution._
import com.signalcollect.evaluation.algorithms._
import com.signalcollect.evaluation.graphs._
import com.signalcollect.configuration._
import com.signalcollect.evaluation.resulthandling._
import com.signalcollect._

object LoadingComparsion extends App {

  val evaluation = new EvaluationSuiteCreator(evaluationName = "Loading Evaluation",
  executionHost = new TorqueHost("strebel"/*System.getProperty("user.name")*/, recompileCore = false))
  
  val centralizedLoading = new PageRankEvaluationRun(executionConfiguration = ExecutionConfiguration(ExecutionMode.OptimizedAsynchronous), graph = new LogNormalGraph(graphSize = 200000))
  
  val distributedLoading = new PageRankEvaluationRun(executionConfiguration = ExecutionConfiguration(ExecutionMode.OptimizedAsynchronous), graph = new DistributedLogNormal(graphSize = 200000, numberOfWorkers = Some(24)))
  
  val distributedRandomly = new PageRankEvaluationRun(executionConfiguration = ExecutionConfiguration(ExecutionMode.OptimizedAsynchronous), graph = new DistributedLogNormal(graphSize = 200000))
  
  for (i <- 0 until 2) {
    evaluation.addJobForEvaluationAlgorithm(centralizedLoading)

    evaluation.addJobForEvaluationAlgorithm(distributedLoading)

    evaluation.addJobForEvaluationAlgorithm(distributedRandomly)

  }
  
  
  evaluation.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  evaluation.runEvaluation()
}