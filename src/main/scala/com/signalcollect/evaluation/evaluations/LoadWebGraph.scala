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
import com.signalcollect.nodeprovisioning.Node
import com.signalcollect.nodeprovisioning.local._

object LoadWebGraph extends App {

  /*
   * Config
   */
  val runName = "JDK8, G1 and all kinds of things"

  val localMode = false
  val locationSplits = if (localMode) "/Users/" + System.getProperty("user.name") + "/webgraph/" else "/home/torque/tmp/webgraph-tmp"
  val loggerFile = if (localMode) Some("/Users/" + System.getProperty("user.name") + "/status.txt") else Some("/home/user/" + System.getProperty("user.name") + "/status.txt")

  val evaluation = new EvaluationSuiteCreator(evaluationName = runName,
    executionHost = if (localMode) {
      new LocalHost()
    } else {
      new TorqueHost(
        torqueHostname = "kraken.ifi.uzh.ch",
        localJarPath = "./target/signal-collect-evaluation-2.0.0-SNAPSHOT-jar-with-dependencies.jar",
        torqueUsername = System.getProperty("user.name"))
    })

  for (splits <- List(24, 24, 24, 24)) {
    evaluation.addJobForEvaluationAlgorithm(new PageRankForWebGraph(
        jvmParams = "-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC -XX:+UseNUMA -XX:+DoEscapeAnalysis",
        jdkBinaryPath = "./jdk1.8.0/bin/",
//      jvmParams = "-XX:+UseNUMA -XX:+UseCondCardMark -XX:+UseParallelGC -XX:+DoEscapeAnalysis", //if (localMode) "" else " -agentpath:./profiler/libyjpagent.so ",  //-XX:+UseNUMA -XX:+UseCondCardMark -XX:+UseParallelGC
      graphBuilder = GraphBuilder,
      //      graphBuilder = GraphBuilder.withNodeProvisioner(new LocalNodeProvisioner {
      //        override def getNodes: List[Node] = {
      //          List(new LocalNode {
      //            override def numberOfCores = 1
      //          })
      //        }
      //      }),
      graphProvider = new WebGraphParserGzip(locationSplits, loggerFile, splitsToParse = splits, numberOfWorkers = if (localMode) 1 else 24),
      runConfiguration = ExecutionConfiguration.withExecutionMode(ExecutionMode.PureAsynchronous),
      dummyVertices = false
    ))
  }

  evaluation.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  evaluation.runEvaluation
}