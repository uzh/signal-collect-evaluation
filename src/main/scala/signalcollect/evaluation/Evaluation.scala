/*
 *  @author Philip Stutz
 *  @author Daniel Strebel
 *  @author Francisco de Freitas
 *  @author Lorenz Fischer
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

package signalcollect.evaluation

import org.apache.commons.codec.binary.Base64
import signalcollect.interfaces._
import signalcollect.configuration._
import signalcollect.evaluation.util.Serializer
import signalcollect.evaluation.spreadsheets._
import java.util.Date
import java.text.SimpleDateFormat
import signalcollect.evaluation.configuration._
import scala.util.Random
import signalcollect.graphproviders.synthetic.LogNormal
import signalcollect.algorithms.Page
import signalcollect.algorithms.Link

object Evaluation extends App {
  var configuration: JobConfiguration = null
  if (args.size > 0) {
    val configurationBase64 = args(0)
    val configurationBytes = Base64.decodeBase64(configurationBase64)
    configuration = Serializer.read[JobConfiguration](configurationBytes)
  } else {
	  throw new Exception("No evaluation configuration specified.")
  }
  val eval = new Evaluation
  eval.execute(configuration)
}

class Evaluation {
  def execute(configuration: JobConfiguration) {
    var statsMap = Map[String, String]()

    val startDate = new Date
    val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
    val timeFormat = new SimpleDateFormat("HH:mm:ss")
    try {
      statsMap += (("startDate", dateFormat.format(startDate)))
      statsMap += (("startTime", timeFormat.format(startDate)))

      statsMap += (("evaluationDescription", configuration.evaluationDescription))
      statsMap += (("submittedByUser", configuration.submittedByUser))
      statsMap += (("jobId", configuration.jobId.toString))
      statsMap += (("executionHostname", java.net.InetAddress.getLocalHost.getHostName))

      configuration match {
        case pageRankConfig: PageRankConfiguration =>
          statsMap += (("algorithm", "PageRank"))
          val computeGraph = pageRankConfig.builder.build
          val seed = 0
          val sigma = 1.0
          val mu = 3.0
          statsMap += (("graphStructure", "LogNormal(" + pageRankConfig.graphSize + ", " + seed + ", " + sigma + ", " + mu + ")"))
          val edgeTuples = new LogNormal(pageRankConfig.graphSize, seed, sigma, mu)
          buildPageRankGraph(computeGraph, edgeTuples)
          benchmark(computeGraph, pageRankConfig.executionConfiguration)
          def buildPageRankGraph(cg: ComputeGraph, edgeTuples: Traversable[Tuple2[Int, Int]]): ComputeGraph = {
            edgeTuples foreach {
              case (sourceId, targetId) =>
                cg.add(new Page(sourceId, 0.85))
                cg.add(new Page(targetId, 0.85))
                cg.add(new Link(sourceId, targetId))
            }
            cg
          }
          if (pageRankConfig.spreadsheetConfiguration.isDefined) {
            submitSpreadsheetRow(pageRankConfig.spreadsheetConfiguration.get, statsMap)
          } else {
            println(statsMap)
          }
        /** ADD OTHER ALGORITHMS HERE */
        case other => statsMap += (("exception", "Unknown algorithm: " + other))
      }
    } catch {
      case e: Exception =>
        statsMap += (("exception", e.getMessage + "\n" + e.getStackTraceString))
        println(statsMap)
    }

    def submitSpreadsheetRow(spreadsheetConfig: SpreadsheetConfiguration, rowData: Map[String, String]) {
      val api = new SpreadsheetApi(spreadsheetConfig.gmailAccount, spreadsheetConfig.gmailPassword)
      val spreadsheet = api.getSpreadsheet(spreadsheetConfig.spreadsheetName)
      val worksheet = api.getWorksheetInSpreadsheet(spreadsheetConfig.worksheetName, spreadsheet)
      api.insertRow(worksheet, statsMap)
    }

    def benchmark(computeGraph: ComputeGraph, executionConfiguration: ExecutionConfiguration) {
      val stats = computeGraph.execute(executionConfiguration)
      statsMap += (("numberOfWorkers", stats.config.numberOfWorkers.toString))
      statsMap += (("computationTimeInMilliseconds", stats.executionStatistics.computationTimeInMilliseconds.toString))
      statsMap += (("jvmCpuTimeInMilliseconds", stats.executionStatistics.jvmCpuTimeInMilliseconds.toString))
      statsMap += (("graphLoadingWaitInMilliseconds", stats.executionStatistics.graphLoadingWaitInMilliseconds.toString))
      statsMap += (("executionMode", stats.parameters.executionMode.toString))
      statsMap += (("storageFactory", stats.config.graphConfiguration.storageFactory.name))
      statsMap += (("messageBusFactory", stats.config.graphConfiguration.messageBusFactory.name))
      statsMap += (("optionalLogger", stats.config.optionalLogger.toString))
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
    }
  }
}

