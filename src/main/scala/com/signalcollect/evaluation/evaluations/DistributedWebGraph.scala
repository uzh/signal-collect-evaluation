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
import com.signalcollect.evaluation.algorithms._
import com.signalcollect.evaluation.util._
import com.signalcollect.configuration._
import com.signalcollect.evaluation.resulthandling._
import com.signalcollect._
import com.signalcollect.nodeprovisioning.Node
import com.signalcollect.nodeprovisioning.local._
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory

object DistributedWebGraph extends App {

  /*
   * Config
   */
  val numberOfNodes = 8
  val splitsList = List(3840)
  val akkaCompression = true
  val repetitions = 1

  val runName = splitsList + " splits on " +
    numberOfNodes + " machines, " +
    repetitions + " repetitions," +
    " compression: " + akkaCompression +
    ", profiling, heartbeat 100ms"

  val locationSplits = "/home/torque/tmp/webgraph-tmp"
  val loggerFile = Some("/home/user/" + System.getProperty("user.name") + "/status.txt")

  val evaluation: EvaluationSuiteCreator = new EvaluationSuiteCreator(
    evaluationName = runName,
    executionHost = new LocalHost()
  )

  val baseOptions =
    " -Xmx64000m" +
      " -Xms64000m" +
      " -Xmn8000m" +
      " -d64" // +
  //" -Dsun.io.serialization.extendedDebugInfo=true"

  for (
    jvmParams <- List(
      " -XX:+UnlockExperimentalVMOptions" +
        " -XX:+UseConcMarkSweepGC" +
        " -XX:+UseParNewGC" +
        " -XX:+CMSIncrementalPacing" +
        " -XX:+CMSIncrementalMode" +
        " -XX:ParallelGCThreads=20" +
        " -XX:ParallelCMSThreads=20" +
        " -XX:MaxInlineSize=1024" +
        " -agentpath:./profiler/libyjpagent.so"
    )
  ) {
    for (repetition <- 1 to repetitions) {
      for (jvm <- List("")) { //, "./jdk1.8.0/bin/"
        for (splits <- splitsList) { //10
          evaluation.addJobForEvaluationAlgorithm(new PageRankForWebGraph(
            memoryStats = false,
            jvmParams = jvmParams + baseOptions,
            jdkBinaryPath = jvm,
            graphBuilder = new GraphBuilder[Int, Float]().
              withConsole(true).
              withMessageBusFactory(new BulkAkkaMessageBusFactory[Int, Float](10000)).
              withAkkaMessageCompression(akkaCompression).
              withHeartbeatInterval(100).
              withNodeProvisioner(new TorqueNodeProvisioner(
                torqueHost = new TorqueHost(
                  torqueHostname = "kraken.ifi.uzh.ch",
                  localJarPath = "./target/signal-collect-evaluation-assembly-2.0.0-SNAPSHOT.jar",
                  torqueUsername = System.getProperty("user.name")), numberOfNodes = numberOfNodes, jvmParameters = baseOptions + jvmParams)),
            graphProvider = new WebGraphParserGzip(locationSplits, loggerFile, splitsToParse = splits, numberOfWorkers = numberOfNodes * 24),
            runConfiguration = ExecutionConfiguration.withExecutionMode(ExecutionMode.PureAsynchronous)
          ))
        }
      }
    }
  }
  evaluation.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  evaluation.runEvaluation
}