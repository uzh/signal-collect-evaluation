/*
 *  @author Daniel Strebel
 *
 *  Copyright 2011 University of Zurich
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
 */
package com.signalcollect.evaluation.jobsubmission

import com.signalcollect._
import com.signalcollect.evaluation.algorithms._
import com.signalcollect.evaluation.util._
import com.signalcollect.evaluation.configuration._
import scala.util.Random
import java.util.Date
import java.text.SimpleDateFormat
import java.io.{ FileWriter, BufferedWriter }

object SSSPEvaluation extends App {
        val executionLocation = LocalHost
//  val executionLocation = Kraken("strebel")
//
  val jobSubmitter = new JobSubmitter(executionLocation = executionLocation)
  val jobGenerator = new SSSPJobGenerator(args(0), args(1))
  val jobs = jobGenerator.generateJobs
  jobSubmitter.submitJobs(jobs)
}

class SSSPJobGenerator(gmailAccount: String, gmailPassword: String) extends Serializable {
  lazy val computeGraphBuilders = List(GraphBuilder) /*List(DefaultComputeGraphBuilder, DefaultComputeGraphBuilder.withMessageBusFactory(messageBus.AkkaBus).withWorkerFactory(worker.AkkaLocal))*/
  lazy val numberOfRepetitions = 2
  //  lazy val numberOfWorkersList = (1 to 24).toList
  lazy val numberOfWorkersList = List(2)
  lazy val executionConfigurations = List(ExecutionConfiguration(), ExecutionConfiguration(executionMode = SynchronousExecutionMode))
  lazy val graphSizes = List(20000)

  def generateJobs: List[Job] = {
    var jobs = List[Job]()
    for (computeGraphBuilder <- computeGraphBuilders) {
      for (executionConfiguration <- executionConfigurations) {
        for (numberOfWorkers <- numberOfWorkersList) {
          for (graphSize <- graphSizes) {
            for (repetition <- 1 to numberOfRepetitions) {
              val seed = 0
              val sigma = 1.3
              val mu = 4.0

              val builder = computeGraphBuilder.withNumberOfWorkers(numberOfWorkers)
              val randomJobID = Random.nextInt.abs
              val job = new Job(
                spreadsheetConfiguration = Some(new SpreadsheetConfiguration(gmailAccount, gmailPassword, "evaluation", "data")),
                submittedByUser = System.getProperty("user.name"),
                jobId = randomJobID,
                jobDescription = "SSSP execution",
                execute = { () =>
                  var statsMap = Map[String, String]()
                  statsMap += (("algorithm", "SSSP"))
                  val computeGraph = builder.build
                  statsMap += (("graphStructure", "LogNormal(" + graphSize + ", " + seed + ", " + sigma + ", " + mu + ")"))

                  val locationFactory = new VertexFactory(new LogNormalParameters(sigma, mu, graphSize))

                  for (id <- (0 until graphSize - 1)) {
                    val location = locationFactory.getLocationForId(id)
                    computeGraph.addVertex(location)
                  }
                  computeGraph.addVertex(new MemoryEfficientLocation(graphSize - 1))

                  val startDate = new Date
                  val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
                  val timeFormat = new SimpleDateFormat("HH:mm:ss")
                  statsMap += (("startDate", dateFormat.format(startDate)))
                  statsMap += (("startTime", timeFormat.format(startDate)))
                  val stats = computeGraph.execute(executionConfiguration)
                  statsMap += (("numberOfWorkers", numberOfWorkers.toString))
                  statsMap += (("computationTimeInMilliseconds", stats.executionStatistics.computationTimeInMilliseconds.toString))
                  statsMap += (("jvmCpuTimeInMilliseconds", stats.executionStatistics.jvmCpuTimeInMilliseconds.toString))
                  statsMap += (("graphIdleWaitingTimeInMilliseconds", stats.executionStatistics.graphIdleWaitingTimeInMilliseconds.toString))
                  statsMap += (("executionMode", stats.parameters.executionMode.toString))
                  statsMap += (("workerFactory", stats.config.workerConfiguration.workerFactory.name))
                  statsMap += (("storageFactory", stats.config.workerConfiguration.storageFactory.name))
                  statsMap += (("messageBusFactory", stats.config.workerConfiguration.messageBusFactory.name))
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
                  val endDate = new Date
                  statsMap += (("endDate", dateFormat.format(endDate)))
                  statsMap += (("endTime", timeFormat.format(endDate)))
                  computeGraph.shutdown
                  statsMap
                })
              jobs = job :: jobs
            }
          }
        }
      }
    }
    jobs
  }
}