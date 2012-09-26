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
import com.signalcollect.evaluation.algorithms.SsspEvaluationRun
import com.signalcollect.configuration._
import com.signalcollect._
import com.signalcollect.nodeprovisioning.torque.TorqueNodeProvisioner
import com.signalcollect.graphproviders.synthetic._
import com.signalcollect.nodeprovisioning.local.LocalNodeProvisioner
import com.signalcollect.nodeprovisioning.Node
import com.signalcollect.nodeprovisioning.local.LocalNode
import com.signalcollect.factory.storage.AboveAverage
import com.signalcollect.evaluation.algorithms.VertexColoringEvaluationRun
import com.signalcollect.evaluation.algorithms.ChineseWhispersEvaluationRun
import com.signalcollect.evaluation.util.ParallelFileGraphLoader
import com.signalcollect.evaluation.util.GoogleGraphLoader

/**
 * Runs a PageRank algorithm on a graph of a fixed size
 * for different numbers of worker threads.
 *
 * Evaluation is set to execute on a 'Kraken'-node.
 */
object MulticoreScalabilityEvaluation extends App {

  val evalName = "Specialization removed eval"
  val jvmParameters = "-XX:+UseNUMA -XX:+UseCondCardMark -XX:+UseParallelGC"

  val slowEval = new EvaluationSuiteCreator(evaluationName = evalName,
    executionHost = new TorqueHost(torqueHostname = "kraken.ifi.uzh.ch", localJarPath = "./target/signal-collect-evaluation-2.0.0-SNAPSHOT-jar-with-dependencies.jar", torqueUsername = System.getProperty("user.name"), priority = TorquePriority.fast))

  val fastEval = new EvaluationSuiteCreator(evaluationName = evalName,
    executionHost = new TorqueHost(torqueHostname = "kraken.ifi.uzh.ch", localJarPath = "./target/signal-collect-evaluation-2.0.0-SNAPSHOT-jar-with-dependencies.jar", torqueUsername = System.getProperty("user.name"), priority = TorquePriority.superfast))

  //  val kraken = new com.signalcollect.nodeprovisioning.torque.TorqueHost(torqueHostname = "kraken.ifi.uzh.ch", localJarPath = "./target/signal-collect-evaluation-2.0.0-SNAPSHOT-jar-with-dependencies.jar", privateKeyFilePath = "/home/user/stutz/.ssh/id_rsa")
  //  val krakenNodeProvisioner = new TorqueNodeProvisioner(kraken, 1)
  //  val graphBuilder = GraphBuilder.withNodeProvisioner(krakenNodeProvisioner) //.withLoggingLevel(LoggingLevel.Debug)

  val executionConfigAsync = ExecutionConfiguration(ExecutionMode.PureAsynchronous).withSignalThreshold(0.01)
  val executionConfigSync = ExecutionConfiguration(ExecutionMode.Synchronous).withSignalThreshold(0.01)
  val aboveAverageScheduler = AboveAverage

  val repetitions = 10
  for (i <- 0 until repetitions) {
    for (executionConfig <- List(executionConfigAsync)) { //executionConfigSync
      //    val graphStructure = new LogNormalGraph(graphSize = 200000)
      //graphSize: Int, seed: Long = 0, sigma: Double = 1, mu: Double = 3
      //    val graphStructureDense = new LogNormalGraph(graphSize = 1000000, seed = 0, sigma = 1, mu = 3)
      //    val graphStructureSparse = new LogNormalGraph(graphSize = 1000000, seed = 0, sigma = 1, mu = 1)
              for (numberOfWorkers <- List(24)) {
//      for (numberOfWorkers <- List(24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)) {
        //    for (numberOfWorkers <- List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)) {
        val graphBuilder = GraphBuilder.withNodeProvisioner(new LocalNodeProvisioner {
          override def getNodes: List[Node] = {
            List(new LocalNode {
              override def numberOfCores = numberOfWorkers
            })
          }
        })
        //        val sparseSmallDirectedGraphLoader = new ParallelFileGraphLoader(numberOfWorkers: Int, vertexFilename = "./lognormal-vertices200000-sigma1-mu1", edgeFilename = "lognormal-edges896737-sigma1-mu1", directed = true)
        //        val sparseSmallUndirectedGraphLoader = new ParallelFileGraphLoader(numberOfWorkers: Int, vertexFilename = "./lognormal-vertices200000-sigma1-mu1", edgeFilename = "lognormal-edges896737-sigma1-mu1", directed = false)
        //        val denseSmallDirectedGraphLoader = new ParallelFileGraphLoader(numberOfWorkers: Int, vertexFilename = "./lognormal-vertices200000-sigma1-mu3", edgeFilename = "lognormal-edges6597583-sigma1-mu3", directed = true)
        //        val denseSmallUndirectedGraphLoader = new ParallelFileGraphLoader(numberOfWorkers: Int, vertexFilename = "./lognormal-vertices200000-sigma1-mu3", edgeFilename = "lognormal-edges6597583-sigma1-mu3", directed = false)
        //        val sparseLargeDirectedGraphLoader = new ParallelFileGraphLoader(numberOfWorkers: Int, vertexFilename = "./lognormal-vertices1000000-sigma1-mu1", edgeFilename = "lognormal-edges4484396-sigma1-mu1", directed = true)
        //        val sparseLargeUndirectedGraphLoader = new ParallelFileGraphLoader(numberOfWorkers: Int, vertexFilename = "./lognormal-vertices1000000-sigma1-mu1", edgeFilename = "lognormal-edges4484396-sigma1-mu1", directed = false)
        //        val denseLargeDirectedGraphLoader = new ParallelFileGraphLoader(numberOfWorkers: Int, vertexFilename = "./lognormal-vertices1000000-sigma1-mu3", edgeFilename = "lognormal-edges33086286-sigma1-mu3", directed = true)
        //        val denseLargeUndirectedGraphLoader = new ParallelFileGraphLoader(numberOfWorkers: Int, vertexFilename = "./lognormal-vertices1000000-sigma1-mu3", edgeFilename = "lognormal-edges33086286-sigma1-mu3", directed = false)
        val googleWebGraph = new GoogleGraphLoader(numberOfWorkers: Int)
        for (graphLoader <- List(googleWebGraph)) { //sparseSmallDirectedGraphLoader, denseSmallDirectedGraphLoaderdenseLargeDirectedGraphLoader, sparseLargeDirectedGraphLoaderdenseSmallDirectedGraphLoader
          //          if (numberOfWorkers <= 2) {
          fastEval.addJobForEvaluationAlgorithm(new PageRankEvaluationRun(graphBuilder = graphBuilder, graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters, reportMemoryStats = true))
//          slowEval.addJobForEvaluationAlgorithm(new SsspEvaluationRun(graphBuilder = graphBuilder, graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters))
//          if (executionConfig == executionConfigAsync) {
//            slowEval.addJobForEvaluationAlgorithm(new SsspEvaluationRun(graphBuilder = graphBuilder.withStorageFactory(aboveAverageScheduler), graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters))
          }
          //          } else {
          //            fastEval.addJobForEvaluationAlgorithm(new PageRankEvaluationRun(graphBuilder = graphBuilder, graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters))
          //            fastEval.addJobForEvaluationAlgorithm(new SsspEvaluationRun(graphBuilder = graphBuilder, graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters))
          //            if (executionConfig == executionConfigAsync) {
          //              fastEval.addJobForEvaluationAlgorithm(new SsspEvaluationRun(graphBuilder = graphBuilder.withStorageFactory(aboveAverageScheduler), graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters))
          //            }
          //          }
        }
        //        for (graphLoader <- List(denseSmallUndirectedGraphLoader)) {
        //          if (numberOfWorkers <= 2) {
        //            slowEval.addJobForEvaluationAlgorithm(new ChineseWhispersEvaluationRun(graphBuilder = graphBuilder, graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters))
        //          } else {
        //            fastEval.addJobForEvaluationAlgorithm(new ChineseWhispersEvaluationRun(graphBuilder = graphBuilder, graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters))
        //          }
        //        }
//      }
    }

  }
  slowEval.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  slowEval.runEvaluation()
  fastEval.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  fastEval.runEvaluation()
}