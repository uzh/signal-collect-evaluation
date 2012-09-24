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
import com.signalcollect.nodeprovisioning.torque._
import com.signalcollect.evaluation.algorithms._
import com.signalcollect.evaluation.util._
import com.signalcollect.configuration._
import com.signalcollect.evaluation.resulthandling._
import com.signalcollect._

object LoadWebGraph extends App {

  /*
   * Config
   */
  val runName = "Loading Webgraph 240th split"
  val locationSplits = "/home/user/strebel/webgraph_part2/"
  val loggerFile = Some("/home/user/strebel/status.txt")

  val evaluation = new EvaluationSuiteCreator(evaluationName = runName,
    executionHost = new TorqueHost(torqueHostname = "kraken.ifi.uzh.ch", localJarPath = "./target/signal-collect-evaluation-2.0.0-SNAPSHOT-jar-with-dependencies.jar", torqueUsername = System.getProperty("user.name")))

  evaluation.addJobForEvaluationAlgorithm(new PageRankForWebGraph(
    graphProvider = new WebGraphParser(locationSplits, loggerFile, (0 until 24)),
    runConfiguration = ExecutionConfiguration.withExecutionMode(ExecutionMode.PureAsynchronous))
  )

  evaluation.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  evaluation.runEvaluation
}