/*
 *  @author Daniel Strebel
 *  @author Philip Stutz
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
import com.signalcollect.nodeprovisioning.torque._
import com.signalcollect.evaluation.resulthandling._
import com.signalcollect.evaluation.algorithms.PageRankEvaluationRun
import com.signalcollect.configuration._
import com.signalcollect._
import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner
import com.signalcollect.graphproviders.synthetic._
import com.signalcollect.nodeprovisioning.local.LocalNodeProvisioner
import com.signalcollect.nodeprovisioning.Node
import com.signalcollect.nodeprovisioning.local.LocalNode
import com.signalcollect.evaluation.algorithms.ChineseWhispersEvaluationRun
import com.signalcollect.evaluation.util.ParallelFileGraphLoader
import com.signalcollect.evaluation.util.GoogleGraphLoader
import com.typesafe.config.Config
import com.signalcollect.factory.worker.DistributedWorker
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory

/**
 * Runs a PageRank algorithm on a graph of a fixed size
 * for different numbers of worker threads.
 *
 * Evaluation is set to execute on a 'Kraken'-node.
 */
object MulticoreScalabilityEvaluation extends App {

  val evalName = "removed the condition on the processing"
  val jvmParameters = " -XX:+UseNUMA -XX:+UseCondCardMark -XX:+UseParallelGC" //-agentpath:./profiler/libyjpagent.so"
  val kraken = new TorqueHost(
                  jobSubmitter = new TorqueJobSubmitter(username = System.getProperty("user.name"), hostname = "kraken.ifi.uzh.ch"),
                  localJarPath = "./target/signal-collect-evaluation-assembly-2.0.0-SNAPSHOT.jar", priority = TorquePriority.superfast)

  val superfastEval = new EvaluationSuiteCreator(evaluationName = evalName,
    executionHost = kraken)

  val executionConfigAsync = ExecutionConfiguration(ExecutionMode.PureAsynchronous).withSignalThreshold(0.01)
  val executionConfigSync = ExecutionConfiguration(ExecutionMode.Synchronous).withSignalThreshold(0.01)

  val repetitions = 2
  val workers = List(24) // 1 to 24
  for (i <- 0 until repetitions) {
    for (executionConfig <- List(executionConfigAsync)) {
      for (numberOfWorkers <- workers) {
        val graphBuilder = new GraphBuilder[Int, Double]().
          withNodeProvisioner(new LocalNodeProvisioner {
            override def getNodes(akkaConfig: Config): List[Node] = {
              List(new LocalNode {
                override def numberOfCores = numberOfWorkers
              })
            }
          })
        val googleWebGraph = new GoogleGraphLoader(numberOfWorkers: Int)
        for (graphLoader <- List(googleWebGraph)) {
          superfastEval.addJobForEvaluationAlgorithm(new PageRankEvaluationRun(graphBuilder = graphBuilder, graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters, reportMemoryStats = false))
        }
      }
    }
  }
  superfastEval.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  superfastEval.runEvaluation
}