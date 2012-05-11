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
import com.signalcollect.evaluation.algorithms.VertexColoring
import com.signalcollect.evaluation.util.QuasigroupGraphLoader

//http://www.google.ch/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&ved=0CHAQFjAA&url=http%3A%2F%2Fmat.gsia.cmu.edu%2FCOLOR04%2FINSTANCES%2Fqg.order100.col&ei=Mw-tT-TXFfKK4gSe88WRDA&usg=AFQjCNHNPRbZUUvxTkkSaV7-k2dCVds44A

object VertexColoringSyncVsAsyncEvaluation extends App {

  val evalName = "VertexColoringSyncVsAsync Quasigroup 100*100 Matrix"
  val jvmParameters = "-XX:+UseNUMA -XX:+UseCondCardMark -XX:+UseParallelGC"

  val fastEval = new EvaluationSuiteCreator(evaluationName = evalName,
    executionHost = new TorqueHost(torqueHostname = "kraken.ifi.uzh.ch", localJarPath = "./target/signal-collect-evaluation-2.0.0-SNAPSHOT-jar-with-dependencies.jar", torqueUsername = System.getProperty("user.name"), priority = TorquePriority.superfast))
 
  val executionConfigAsync = ExecutionConfiguration(ExecutionMode.PureAsynchronous).withSignalThreshold(0.01).withTimeLimit(1200000)
  val executionConfigSync = ExecutionConfiguration(ExecutionMode.Synchronous).withSignalThreshold(0.01).withTimeLimit(1200000)

  val repetitions = 10
  for (i <- 0 until repetitions) {
    for (numColors <- List(50000, 5000, 1000, 500, 200, 150, 140, 130, 125, 120, 118, 116, 114, 112, 110, 109, 108, 107, 106, 105, 104, 103, 102, 101, 100))
      for (executionConfig <- List(executionConfigAsync, executionConfigSync)) {
        val graphBuilder = GraphBuilder
        val googleWebGraph = new QuasigroupGraphLoader(24: Int, edgeFilename = "quasigroup100", directed = false)
        for (graphLoader <- List(googleWebGraph)) {
          fastEval.addJobForEvaluationAlgorithm(new VertexColoringEvaluationRun(numColors = numColors, graphBuilder = graphBuilder, graphProvider = graphLoader, executionConfiguration = executionConfig, jvmParams = jvmParameters), Map("numColors" -> numColors.toString))
        }
      }
  }
  fastEval.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  fastEval.runEvaluation()
}