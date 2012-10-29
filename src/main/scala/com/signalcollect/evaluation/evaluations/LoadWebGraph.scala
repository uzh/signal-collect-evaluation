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
import com.typesafe.config.Config

object LoadWebGraph extends App {

  /*
   * Config
   */
  val runName = "BULKING ENABLED - NEW EDGE FORMAT - 768/960 splits"

  val localMode = false
  val locationSplits = if (localMode) "/Users/" + System.getProperty("user.name") + "/webgraph/" else "/home/torque/tmp/webgraph-tmp"
  val loggerFile = if (localMode) Some("/Users/" + System.getProperty("user.name") + "/status.txt") else Some("/home/user/" + System.getProperty("user.name") + "/status.txt")

  val evaluation: EvaluationSuiteCreator = new EvaluationSuiteCreator(evaluationName = runName,
    executionHost = if (localMode) {
      new LocalHost()
    } else {
      new TorqueHost(
        torqueHostname = "kraken.ifi.uzh.ch",
        localJarPath = "./target/signal-collect-evaluation-2.0.0-SNAPSHOT-jar-with-dependencies.jar",
        torqueUsername = System.getProperty("user.name"))
    })

  // To test: -XX:+AggressiveOpts
  // -XX:+UseBiasedLocking
  //-XX:ParallelGCThreads=20 +
  //XX:ParallelCMSThreads=n +
  //-XX:+CMSIncrementalMode +
  //-XX:+CMSIncrementalPacing +
  //-XX:+AggressiveHeap -> crahes, i guess

  // BEST SO FAR: "-XX:+UnlockExperimentalVMOptions -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSIncrementalPacing -XX:+CMSIncrementalMode -XX:ParallelGCThreads=20 -XX:ParallelCMSThreads=20 -XX:+UseNUMA -XX:+UseFastAccessorMethods", 
  //"-XX:+UnlockExperimentalVMOptions -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSIncrementalPacing -XX:+CMSIncrementalMode -XX:ParallelGCThreads=20 -XX:ParallelCMSThreads=20 -XX:+UseNUMA -XX:+UseFastAccessorMethods"
  //"-XX:+UnlockExperimentalVMOptions -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSIncrementalPacing -XX:+CMSIncrementalMode"
  //jvmParams = "-XX:+UseNUMA -XX:+UseCondCardMark -XX:+UseParallelGC -XX:+DoEscapeAnalysis", //if (localMode) "" else " -agentpath:./profiler/libyjpagent.so ",  //-XX:+UseNUMA -XX:+UseCondCardMark -XX:+UseParallelGC
  //-XX:AutoBoxCacheMax=10000  -XX:MaxGCPauseMillis=50 -XX:+UseG1GC -XX:+DoEscapeAnalysis -XX:+UseNUMA -XX:+UseG1GC -XX:+UseNUMA -XX:+DoEscapeAnalysis -agentpath:./profiler/libyjpagent.so -XX:+UseCondCardMark -XX:+UseParallelGC
  //"-XX:+UnlockExperimentalVMOptions -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseTLAB", "-XX:+UnlockExperimentalVMOptions -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+DoEscapeAnalysis -XX:+UseCondCardMark"
  //"-XX:ParallelGCThreads=20 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:SurvivorRatio=8 -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=31"
  val baseOptions =
    " -Xmx64000m" +
      " -Xms64000m" +
      " -Xmn8000m" +
      " -d64"
  val repetitions = 3
  for (
    jvmParams <- List(
      " -XX:+UnlockExperimentalVMOptions" +
        " -XX:+UseConcMarkSweepGC" +
        " -XX:+UseParNewGC" +
        " -XX:+CMSIncrementalPacing" +
        " -XX:+CMSIncrementalMode" +
        " -XX:ParallelGCThreads=20" +
        " -XX:ParallelCMSThreads=20" +
//        " -Xincgc" +
//        " -XX:-DontCompileHugeMethods" +
        " -XX:MaxInlineSize=1024" //+
//        " -XX:FreqInlineSize=1024"
    //" -agentpath:./profiler/libyjpagent.so"
    )
  ) {
    for (repetition <- 1 to repetitions) {
      for (jvm <- List("")) { //, "./jdk1.8.0/bin/"
        for (splits <- List(768, 960)) { //480
          evaluation.addJobForEvaluationAlgorithm(new PageRankForWebGraph(
            memoryStats = false,
            jvmParams = jvmParams + baseOptions,
            jdkBinaryPath = jvm,
            graphBuilder = if (localMode) {
              new GraphBuilder[Int, Float]().withNodeProvisioner(new LocalNodeProvisioner {
                override def getNodes(akkaConfig: Config): List[Node] = {
                  List(new LocalNode {
                    override def numberOfCores = 4
                  })
                }
              }).withMessageBusFactory(new BulkAkkaMessageBusFactory(1000))
            } else {
              new GraphBuilder[Int, Float]().withWorkerFactory(factory.worker.CollectFirstAkka).withMessageBusFactory(new BulkAkkaMessageBusFactory(10000))
            },
            graphProvider = new WebGraphParserGzip(locationSplits, loggerFile, splitsToParse = splits, numberOfWorkers = if (localMode) 4 else 24),
            runConfiguration = ExecutionConfiguration.withExecutionMode(ExecutionMode.PureAsynchronous)
          ))
        }
      }
    }
  }

  evaluation.setResultHandlers(List(new ConsoleResultHandler(true), new GoogleDocsResultHandler(args(0), args(1), "evaluation", "data")))
  evaluation.runEvaluation
}