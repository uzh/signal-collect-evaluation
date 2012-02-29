/*
 *  @author Daniel Strebel
 *  
 *  Copyright 2012 University of Zurich
 *      
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 */

package com.signalcollect.evaluation.evaluations

import com.signalcollect.evaluation.jobsubmission._
import com.signalcollect.evaluation.jobexecution._
import com.signalcollect.evaluation.resulthandling._
import com.signalcollect.evaluation.algorithms.PageRankEvaluationRun
import com.signalcollect.configuration._
import com.signalcollect._
import com.signalcollect.evaluation.graphs.LogNormalGraph
import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner

/**
 * Runs a PageRank algorithm on a graph of a fixed size
 * for different numbers of worker threads.
 *
 * Evaluation is set to execute on a 'Kraken'-node.
 */
object ScalabilityEvaluation extends App {

  val evaluation = new EvaluationSuiteCreator(evaluationName = "Scalability Evaluation Akka RC3",
    executionHost = new TorqueHost(System.getProperty("user.name"), recompileCore = false))

  val repetitions = 10
  for (i <- 0 until repetitions) {
    val graphStructure = new LogNormalGraph(graphSize = 200000)
    val executionConfig = ExecutionConfiguration(ExecutionMode.OptimizedAsynchronous).withSignalThreshold(0.01)
    //    val kraken = new com.signalcollect.nodeprovisioning.torque.TorqueHost(torqueHostname = "kraken.ifi.uzh.ch", localJarPath = "./target/signal-collect-evaluation-2.0.0-SNAPSHOT-jar-with-dependencies.jar", privateKeyFilePath = "/home/user/stutz/.ssh/id_rsa")
    //    val krakenNodeProvisioner = new TorqueNodeProvisioner(kraken, 1)
    val graphBuilder = GraphBuilder //.withNodeProvisioner(krakenNodeProvisioner) //.withLoggingLevel(LoggingLevel.Debug)

    evaluation.addJobForEvaluationAlgorithm(new PageRankEvaluationRun(graphBuilder = graphBuilder, graph = graphStructure, executionConfiguration = executionConfig))
  }

  evaluation.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  evaluation.runEvaluation()
}