/*
 *  @author Philip Stutz
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
 *  
 */

package com.signalcollect.evaluation.jobsubmission

import com.signalcollect.api._
import com.signalcollect.api.factory._
import com.signalcollect.configuration._
import com.signalcollect.evaluation.configuration._
import com.signalcollect.implementations.logging.DefaultLogger
import com.signalcollect.graphproviders.synthetic.LogNormal
import com.signalcollect.examples.Page
import com.signalcollect.examples.Link

import java.util.Date
import java.text.SimpleDateFormat

import scala.util.Random

/*
 * Packages the application, deploys the benchmarking jar/script to kraken
 * and then executes it via torque.
 * 
 * REQUIRES CERTIFICATE FOR LOGIN ON KRAKEN 
 */
object PageRankEvaluation extends App {
  //  val executionLocation = LocalHost
  val executionLocation = Kraken(/*System.getProperty("user.name")*/"defreitas")

  val jobSubmitter = new JobSubmitter(executionLocation = executionLocation)
  val jobGenerator = new PageRankJobGenerator(args(0), args(1))
  val jobs = jobGenerator.generateJobs
  jobSubmitter.submitJobs(jobs)
}

class PageRankJobGenerator(gmailAccount: String, gmailPassword: String) extends Serializable {
  lazy val computeGraphBuilders = List(DefaultComputeGraphBuilder) /*List(DefaultComputeGraphBuilder, DefaultComputeGraphBuilder.withMessageBusFactory(messageBus.AkkaBus).withWorkerFactory(worker.AkkaLocal))*/
  lazy val numberOfRepetitions = 10
//  lazy val numberOfWorkersList = (1 to 24).toList
  lazy val numberOfWorkersList = List(24)
  lazy val executionConfigurations = List(ExecutionConfiguration(), ExecutionConfiguration(executionMode = SynchronousExecutionMode))

  def generateJobs: List[Job] = {
    var jobs = List[Job]()
    for (computeGraphBuilder <- computeGraphBuilders) {
      for (executionConfiguration <- executionConfigurations) {
        for (numberOfWorkers <- numberOfWorkersList) {
          for (repetition <- 1 to numberOfRepetitions) {
            val graphSize = 200000
            val seed = 0
            val sigma = 1.0
            val mu = 3.0
            val builder = computeGraphBuilder.withNumberOfWorkers(numberOfWorkers)
            val job = new Job(
              spreadsheetConfiguration = Some(new SpreadsheetConfiguration(gmailAccount, gmailPassword, "evaluation.francisco", "data")),
              submittedByUser = System.getProperty("user.name"),
              jobId = Random.nextInt.abs,
              jobDescription = "new performance run before commits, interface changes",
              execute = { () =>
                var statsMap = Map[String, String]()
                statsMap += (("algorithm", "PageRank"))
                val computeGraph = builder.build.get
                statsMap += (("graphStructure", "LogNormal(" + graphSize + ", " + seed + ", " + sigma + ", " + mu + ")"))
                val edgeTuples = new LogNormal(graphSize, seed, sigma, mu)
                edgeTuples foreach {
                  case (sourceId, targetId) =>
                    computeGraph.addVertex(new Page(sourceId, 0.85))
                    computeGraph.addVertex(new Page(targetId, 0.85))
                    computeGraph.addEdge(new Link(sourceId, targetId))
                }
                val startDate = new Date
                val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
                val timeFormat = new SimpleDateFormat("HH:mm:ss")
                statsMap += (("startDate", dateFormat.format(startDate)))
                statsMap += (("startTime", timeFormat.format(startDate)))
                val stats = computeGraph.execute(executionConfiguration)
                statsMap += (("numberOfWorkers", numberOfWorkers.toString))
                statsMap += (("computationTimeInMilliseconds", stats.executionStatistics.computationTimeInMilliseconds.toString))
                statsMap += (("jvmCpuTimeInMilliseconds", stats.executionStatistics.jvmCpuTimeInMilliseconds.toString))
                statsMap += (("graphLoadingWaitInMilliseconds", stats.executionStatistics.graphLoadingWaitInMilliseconds.toString))
                statsMap += (("executionMode", stats.parameters.executionMode.toString))
                statsMap += (("workerFactory", stats.config.workerConfiguration.workerFactory.name))
                statsMap += (("storageFactory", stats.config.workerConfiguration.storageFactory.name))
                statsMap += (("messageBusFactory", stats.config.workerConfiguration.messageBusFactory.name))
                statsMap += (("customLogger", stats.config.customLogger.toString))
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
    jobs
  }
}