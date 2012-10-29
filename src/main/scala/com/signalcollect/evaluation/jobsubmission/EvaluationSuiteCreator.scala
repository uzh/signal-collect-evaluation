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

package com.signalcollect.evaluation.jobsubmission

import scala.util.Random
import java.util.Date
import java.text.SimpleDateFormat
import com.signalcollect.ExecutionInformation
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import com.signalcollect.nodeprovisioning.torque._
import com.signalcollect.evaluation.resulthandling._
import com.signalcollect.evaluation.algorithms._
import collection.JavaConversions._
import java.lang.management.ManagementFactory

class EvaluationSuiteCreator(evaluationName: String,
                             evaluationCreator: String = System.getProperty("user.name"),
                             executionHost: ExecutionHost = new LocalHost) {

  val jobs = ListBuffer[TorqueJob]()

  /**
   * Create a job to be run later
   */
  def addJobForEvaluationAlgorithm(run: EvaluationAlgorithmRun[_, _], extraInformation: Map[String, String] = Map[String, String]()) {
    jobs += buildEvaluationJob(run, extraInformation)
  }

  /**
   * Add a result handler that takes care of the evaluation results
   */
  def setResultHandlers(handlers: List[ResultHandler]) {
    executionHost.setResultHandlers(handlers)
  }

  /**
   * Dispatch jobs for execution
   */
  def runEvaluation() = {
    executionHost.executeJobs(jobs.toList)
  }

  def buildEvaluationJob(run: EvaluationAlgorithmRun[_, _], extraInformation: Map[String, String]): TorqueJob = new TorqueJob(
    submittedByUser = evaluationCreator,
    jobId = Random.nextInt.abs,
    jobDescription = evaluationName,
    jvmParameters = run.jvmParameters,
    jdkBinPath = run.jdkBinPath,
    execute = { () =>
      var statsMap = extraInformation

      //Load Graph
      val graphLoadingStart = System.nanoTime
      run.loadGraph
      run.awaitIdle
      val graphLoadingStop = System.nanoTime
      val graphLoadingTime = new FiniteDuration(graphLoadingStop - graphLoadingStart, TimeUnit.NANOSECONDS)
      statsMap += (("graphLoadingTimeInMilliseconds", graphLoadingTime.toMillis.toString))

      val startDate = new Date
      val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
      val timeFormat = new SimpleDateFormat("HH:mm:ss")
      statsMap += (("startDate", dateFormat.format(startDate)))
      statsMap += (("startTime", timeFormat.format(startDate)))

      //Execute the algorithm
      val externallyMeasuredExecutionStartTime = System.nanoTime
      val stats = run.execute
      val externallyMeasuredExecutionStopTime = System.nanoTime
      val externallyMeasuredExecutionTime = externallyMeasuredExecutionStopTime - externallyMeasuredExecutionStartTime
      val externallyMeasuredExecutionTimeInMilliseconds = externallyMeasuredExecutionTime / 1000000l

      statsMap += (("externallyMeasuredExecutionTimeInMilliseconds", externallyMeasuredExecutionTimeInMilliseconds.toString))
      statsMap += (("algorithm", run.algorithmName))
      statsMap += (("graphStructure", run.graphStructure))
      statsMap += (("jvmParameters", run.jvmParameters))
      var gcCount = 0l
      var gcTimeMilliseconds = 0l
      for (gc <- ManagementFactory.getGarbageCollectorMXBeans) {
        gcCount += gc.getCollectionCount
        gcTimeMilliseconds += gc.getCollectionTime
      }
      statsMap += (("gcCount", gcCount.toString))
      statsMap += (("gcTimeMilliseconds", gcTimeMilliseconds.toString))
      if (stats != null) {
        statsMap += (("numberOfWorkers", stats.numberOfWorkers.toString))
        statsMap += (("computationTimeInMilliseconds", stats.executionStatistics.computationTime.toMillis.toString))
        statsMap += (("jvmCpuTimeInMilliseconds", stats.executionStatistics.jvmCpuTime.toMillis.toString))
        statsMap += (("gcTimePercentage", (100 * gcTimeMilliseconds / stats.executionStatistics.jvmCpuTime.toMillis).floor.toInt.toString))
        statsMap += (("graphIdleWaitingTimeInMilliseconds", stats.executionStatistics.graphIdleWaitingTime.toMillis.toString))
        statsMap += (("totalExecutionTimeInMilliseconds", stats.executionStatistics.totalExecutionTime.toMillis.toString))
        statsMap += (("terminationReason", stats.executionStatistics.terminationReason.toString))
        statsMap += (("executionMode", stats.parameters.executionMode.toString))
        statsMap += (("workerFactory", stats.config.workerFactory.name))
        statsMap += (("storageFactory", stats.config.storageFactory.name))
        statsMap += (("messageBusFactory", stats.config.messageBusFactory.name))
        statsMap += (("logger", stats.config.logger.toString))
        statsMap += (("signalSteps", stats.executionStatistics.signalSteps.toString))
        statsMap += (("collectSteps", stats.executionStatistics.collectSteps.toString))
        statsMap += (("numberOfVertices", stats.aggregatedWorkerStatistics.numberOfVertices.toString))
        statsMap += (("numberOfEdges", stats.aggregatedWorkerStatistics.numberOfOutgoingEdges.toString))
        statsMap += (("totalMessagesReceived", stats.aggregatedWorkerStatistics.messagesReceived.toString))
        statsMap += (("collectOperationsExecuted", stats.aggregatedWorkerStatistics.collectOperationsExecuted.toString))
        statsMap += (("signalOperationsExecuted", stats.aggregatedWorkerStatistics.signalOperationsExecuted.toString))
        statsMap += (("stepsLimit", stats.parameters.stepsLimit.toString))
        statsMap += (("signalThreshold", stats.parameters.signalThreshold.toString))
        statsMap += (("collectThreshold", stats.parameters.collectThreshold.toString))
      }

      val endDate = new Date
      statsMap += (("endDate", dateFormat.format(endDate)))
      statsMap += (("endTime", timeFormat.format(endDate)))

      val runtime = Runtime.getRuntime
      val freeMemoryPercentage = ((runtime.freeMemory.toDouble / runtime.totalMemory.toDouble) * 100.0).floor.toInt
      statsMap += (("freeMemoryPercentage", freeMemoryPercentage.toString()))
      val usedMemoryBeforeGc = ((runtime.totalMemory - runtime.freeMemory) / 1073741824.0).ceil.toInt
      statsMap += (("usedMemoryBeforeGc", usedMemoryBeforeGc.toString()))
      val totalMemory = (runtime.totalMemory / 1073741824.0).floor.toInt
      statsMap += (("totalMemory", totalMemory.toString()))
      if (run.memoryStatsEnabled) {
        for (i <- 0 until 10) {
          System.gc
        }
        val usedMemory = ((runtime.totalMemory - runtime.freeMemory) / 1073741824.0).ceil.toInt
        statsMap += (("memory", usedMemory.toString()))
      }

      run.shutdown

      val allDone = System.nanoTime
      val totalRunningTime = new FiniteDuration(allDone - graphLoadingStart, TimeUnit.NANOSECONDS)
      statsMap += (("totalRunningTime", totalRunningTime.toSeconds.toString))
      statsMap
    })

}