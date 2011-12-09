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

import com.signalcollect._
import com.signalcollect.factory._
import com.signalcollect.configuration._
import com.signalcollect.evaluation.configuration._
import com.signalcollect.evaluation.util._
import com.signalcollect.implementations.logging.DefaultLogger
import com.signalcollect.graphproviders.synthetic.LogNormal
import com.signalcollect.graphproviders.synthetic.Partitions
import com.signalcollect.evaluation.algorithms._
import java.util.Date
import java.text.SimpleDateFormat
import scala.util.Random
import com.signalcollect.implementations.logging.DefaultLogger
import com.signalcollect.graphproviders.synthetic.ErdosRenyi

/*
 * Packages the application, deploys the benchmarking jar/script to kraken
 * and then executes it via torque.
 * 
 * REQUIRES CERTIFICATE FOR LOGIN ON KRAKEN 
 */
object PageRankEvaluation extends App {
  //      val executionLocation = LocalHost
  val executionLocation = Kraken(System.getProperty("user.name"))
  //  lazy val recompileCore = true
  lazy val recompileCore = false
  //  val jvmParameters = "-agentpath:/home/user/stutz/libyjpagent.so=port=10001,tracing,noj2ee,monitors,dir=/home/user/stutz/Snapshots"
  val jvmParameters = ""
  //  val jvmParameters = "-XX:+AggressiveHeap"
  val jobSubmitter = new JobSubmitter(mailAddress = mailAddress, jvmParameters = jvmParameters, executionLocation = executionLocation, recompileCore = recompileCore)
  val jobGenerator = new PageRankJobGenerator(args(0), args(1))
  val mailAddress = args(2)
  val jobs = jobGenerator.generateJobs
  jobSubmitter.submitJobs(jobs)
}

class PageRankJobGenerator(gmailAccount: String, gmailPassword: String) extends Serializable {
  lazy val computeGraphBuilders = List(GraphBuilder.withLogger(new DefaultLogger))
    lazy val numberOfRepetitions = 1
//  lazy val numberOfRepetitions = 10
//        lazy val numberOfWorkersList = (1 to 24).toList
  lazy val numberOfWorkersList = List(24)
  lazy val signalThreshold = 0.01
  //  lazy val numberOfWorkersList = List(1, 24)
  lazy val executionConfigurations = List(ExecutionConfiguration().withSignalThreshold(signalThreshold), ExecutionConfiguration(executionMode = ExecutionMode.Synchronous).withSignalThreshold(signalThreshold))
  //  lazy val executionConfigurations = List(ExecutionConfiguration(executionMode = SynchronousExecutionMode))
  //  lazy val executionConfigurations = List(ExecutionConfiguration())
  lazy val graphSizes = List(500000)
  //  lazy val graphSizes = List(200000)
  //  lazy val graphSizes = List(10000)
  //    lazy val graphSizes = List(100)

  def generateJobs: List[Job] = {
    var jobs = List[Job]()
    for (computeGraphBuilder <- computeGraphBuilders) {
      for (executionConfiguration <- executionConfigurations) {
        for (repetition <- 1 to numberOfRepetitions) {
          for (graphSize <- graphSizes) {
            for (numberOfWorkers <- numberOfWorkersList) {
              val seed = 0
              val sigma = 1.3
              val mu = 4.0
              //              val edgeProbability = 10 / graphSize
              val builder = computeGraphBuilder.withNumberOfWorkers(numberOfWorkers)
              val job = new Job(
                spreadsheetConfiguration = Some(new SpreadsheetConfiguration(gmailAccount, gmailPassword, "evaluation", "data")),
                submittedByUser = System.getProperty("user.name"),
                jobId = Random.nextInt.abs,
                jobDescription = "pagerank problem-size-scalability evaluation on log-normal graph",
                execute = { () =>
                  val jobStart = System.nanoTime
                  val startDate = new Date
                  val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
                  val timeFormat = new SimpleDateFormat("HH:mm:ss")
                  var statsMap = Map[String, String]()
                  statsMap += (("algorithm", "PageRank"))
                  val computeGraph = builder.build
                  //                  statsMap += (("graphStructure", "ErdosRenyi(" + graphSize + ", " + edgeProbability + ")"))
                  statsMap += (("graphStructure", "LogNormal(" + graphSize + ", " + seed + ", " + sigma + ", " + mu + ")"))
                  val edgeTuples = new LogNormal(graphSize, seed, sigma, mu)
                  //                  val edgeTuples = new Partitions(24, graphSize, seed, sigma, mu)
                  //                  val edgeTuples = new ErdosRenyi(graphSize, edgeProbability)

                  val graphLoadingStart = System.nanoTime
                  val preGraphLoadingTime = graphLoadingStart - jobStart
                  val preGraphLoadingTimeInMilliseconds = preGraphLoadingTime / 1000000l

                  for (id <- (0 until graphSize).par) {
                    computeGraph.addVertex(new PageRankVertex(id, 0.15))
                  }

                  edgeTuples.par foreach {
                    case (sourceId, targetId) =>
                      computeGraph.addEdge(new PageRankEdge(sourceId, targetId))
                  }
                  val graphLoadingStop = System.nanoTime
                  val graphLoadingTime = graphLoadingStop - graphLoadingStart
                  val graphLoadingTimeInMilliseconds = graphLoadingTime / 1000000l

                  //                  //Reduced message traffic version for loading the graph
                  //                  
                  //                  val pageFactory = new VertexFactory(new LogNormalParameters(sigma, mu, graphSize))
                  //                  for(id<-0 until graphSize) {
                  //                    computeGraph.addVertex(pageFactory.getPageForId(id))
                  //                  }                
                  //

                  statsMap += (("startDate", dateFormat.format(startDate)))
                  statsMap += (("startTime", timeFormat.format(startDate)))

                  val externallyMeasuredExecutionStartTime = System.nanoTime
                  val stats = computeGraph.execute(executionConfiguration)
                  val externallyMeasuredExecutionStopTime = System.nanoTime
                  val externallyMeasuredExecutionTime = externallyMeasuredExecutionStopTime - externallyMeasuredExecutionStartTime
                  val externallyMeasuredExecutionTimeInMilliseconds = externallyMeasuredExecutionTime / 1000000l

                  computeGraph.shutdown

                  statsMap += (("numberOfWorkers", numberOfWorkers.toString))
                  statsMap += (("computationTimeInMilliseconds", stats.executionStatistics.computationTimeInMilliseconds.toString))
                  statsMap += (("jvmCpuTimeInMilliseconds", stats.executionStatistics.jvmCpuTimeInMilliseconds.toString))
                  statsMap += (("graphIdleWaitingTimeInMilliseconds", stats.executionStatistics.graphIdleWaitingTimeInMilliseconds.toString))
                  statsMap += (("graphLoadingTimeInMilliseconds", (graphLoadingTimeInMilliseconds + stats.executionStatistics.graphIdleWaitingTimeInMilliseconds).toString))
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
                  statsMap += (("preExecutionGcTimeInMilliseconds", stats.executionStatistics.preExecutionGcTimeInMilliseconds.toString))
                  statsMap += (("terminationReason", stats.executionStatistics.terminationReason.toString))
                  val endDate = new Date
                  statsMap += (("endDate", dateFormat.format(endDate)))
                  statsMap += (("endTime", timeFormat.format(endDate)))
                  val jobStop = System.nanoTime
                  val jobExecutionTime = jobStop - jobStart
                  val jobExecutionTimeInMilliseconds = jobExecutionTime / 1000000l
                  statsMap += (("jobExecutionTimeInMilliseconds", jobExecutionTimeInMilliseconds.toString))
                  statsMap += (("totalExecutionTimeInMilliseconds", stats.executionStatistics.totalExecutionTimeInMilliseconds.toString))
                  statsMap += (("preGraphLoadingTimeInMilliseconds", preGraphLoadingTimeInMilliseconds.toString))
                  statsMap += (("externallyMeasuredExecutionTimeInMilliseconds", externallyMeasuredExecutionTimeInMilliseconds.toString))
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