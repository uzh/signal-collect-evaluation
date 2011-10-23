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

package com.signalcollect

import org.specs2.mutable._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.matcher.Matcher
import org.specs2.mock.Mockito
import com.signalcollect.interfaces._
import java.util.Map.Entry
import com.signalcollect._
import org.apache.commons.codec.binary.Base64
import com.signalcollect.implementations.serialization.CompressingSerializer
import com.signalcollect.evaluation.configuration.Job
import com.signalcollect.evaluation.configuration.SpreadsheetConfiguration
import com.signalcollect.graphproviders.synthetic.LogNormal
import com.signalcollect.evaluation.algorithms.PageRankVertex
import com.signalcollect.evaluation.algorithms.PageRankEdge
import java.util.Date
import java.text.SimpleDateFormat

@RunWith(classOf[JUnitRunner])
class SerializationSpec extends SpecificationWithJUnit with Mockito {

  "CompressingSerializer" should {

    var compressedSerializedJob: Array[Byte] = null
    var base64encodedCompressedSerializedJob: String = null
    var base64decodedCompressedSerializedJob: Array[Byte] = null
    var deserializedJob: Job = null

    "successfully compress a job" in {
      compressedSerializedJob = CompressingSerializer.write(job)
      true
    }

    "successfully encode a job" in {
      base64encodedCompressedSerializedJob = Base64.encodeBase64String(compressedSerializedJob).replace("\n", "").replace("\r", "")
      true
    }

    "successfully decode a job" in {
      base64decodedCompressedSerializedJob = Base64.decodeBase64(base64encodedCompressedSerializedJob)
      true
    }

    "successfully deserialize a job" in {
      deserializedJob = CompressingSerializer.read[Job](base64decodedCompressedSerializedJob)
      deserializedJob.jobDescription == job.jobDescription
    }

  }

  val job = new Job(
    100,
    Some(SpreadsheetConfiguration("some.emailAddress@gmail.com", "somePasswordHere", "someSpreadsheetNameHere", "someWorksheetNameHere")),
    "someUsername",
    "someJobDescription",
    () => {
      var statsMap = Map[String, String]()
      statsMap += (("algorithm", "PageRank"))
      val computeGraph = GraphBuilder.build
      statsMap += (("graphStructure", "LogNormal(" + 200000 + ", " + 0 + ", " + 1.0 + ", " + 3.0 + ")"))
      val edgeTuples = new LogNormal(200000, 0, 1.0, 3.0)
      edgeTuples foreach {
        case (sourceId, targetId) =>
          computeGraph.addVertex(new PageRankVertex(sourceId, 0.85))
          computeGraph.addVertex(new PageRankVertex(targetId, 0.85))
          computeGraph.addEdge(new PageRankEdge(sourceId, targetId))
      }
      val startDate = new Date
      val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
      val timeFormat = new SimpleDateFormat("HH:mm:ss")
      statsMap += (("startDate", dateFormat.format(startDate)))
      statsMap += (("startTime", timeFormat.format(startDate)))
      val stats = computeGraph.execute(ExecutionConfiguration(executionMode = SynchronousExecutionMode))
      statsMap += (("numberOfWorkers", 24.toString))
      statsMap += (("computationTimeInMilliseconds", stats.executionStatistics.computationTimeInMilliseconds.toString))
      statsMap += (("jvmCpuTimeInMilliseconds", stats.executionStatistics.jvmCpuTimeInMilliseconds.toString))
      statsMap += (("graphLoadingWaitInMilliseconds", stats.executionStatistics.graphLoadingWaitInMilliseconds.toString))
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
      computeGraph.shutdown
      statsMap
    })

}