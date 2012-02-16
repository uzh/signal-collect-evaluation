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
import akka.util.FiniteDuration
import java.util.concurrent.TimeUnit
import com.signalcollect.evaluation.jobexecution._
import com.signalcollect.evaluation.resulthandling._
import com.signalcollect.evaluation.algorithms._

class EvaluationSuiteCreator(evaluationName: String,
  evaluationCreator: String = System.getProperty("user.name"),
  executionHost: ExecutionHost = new LocalHost) {

  val jobs = ListBuffer[Job]()

  /**
   * Create a job to be run later
   */
  def addJobForEvaluationAlgorithm(run: EvaluationAlgorithmRun) {
    jobs += buildEvaluationJob(run)
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

  def buildEvaluationJob(run: EvaluationAlgorithmRun): Job = new Job(
    submittedByUser = evaluationCreator,
    jobId = Random.nextInt.abs,
    jobDescription = evaluationName,
    execute = { () =>
      var statsMap = Map[String, String]()

      //Load Graph
      val graphLoadingStart = System.nanoTime
      run.loadGraph
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

      statsMap += (("algorithm", run.algorithmName))
      statsMap += (("graphStructure", run.graphStructure))
      if (stats != null) {
        statsMap += (("numberOfWorkers", stats.config.numberOfWorkers.toString))
        statsMap += (("computationTimeInMilliseconds", stats.executionStatistics.computationTime.toMillis.toString))
        statsMap += (("jvmCpuTimeInMilliseconds", stats.executionStatistics.jvmCpuTime.toMillis.toString))
        statsMap += (("graphIdleWaitingTimeInMilliseconds", stats.executionStatistics.graphIdleWaitingTime.toMillis.toString))
        statsMap += (("totalExecutionTimeInMilliseconds", stats.executionStatistics.totalExecutionTime.toMillis.toString))
        statsMap += (("executionMode", stats.parameters.executionMode.toString))
        statsMap += (("workerFactory", stats.config.workerFactory.name))
        statsMap += (("storageFactory", stats.config.storageFactory.name))
        statsMap += (("messageBusFactory", stats.config.messageBusFactory.name))
        statsMap += (("logger", stats.config.logger.toString))
        statsMap += (("signalSteps", stats.executionStatistics.signalSteps.toString))
        statsMap += (("collectSteps", stats.executionStatistics.collectSteps.toString))
        statsMap += (("numberOfVertices", stats.aggregatedWorkerStatistics.numberOfVertices.toString))
        statsMap += (("numberOfEdges", stats.aggregatedWorkerStatistics.numberOfOutgoingEdges.toString))
        statsMap += (("collectOperationsExecuted", stats.aggregatedWorkerStatistics.collectOperationsExecuted.toString))
        statsMap += (("signalOperationsExecuted", stats.aggregatedWorkerStatistics.signalOperationsExecuted.toString))
        statsMap += (("stepsLimit", stats.parameters.stepsLimit.toString))
        statsMap += (("signalThreshold", stats.parameters.signalThreshold.toString))
        statsMap += (("collectThreshold", stats.parameters.collectThreshold.toString))
      }
      val endDate = new Date
      statsMap += (("endDate", dateFormat.format(endDate)))
      statsMap += (("endTime", timeFormat.format(endDate)))
      run.shutdown

      statsMap
    })

}