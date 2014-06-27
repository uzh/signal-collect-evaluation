package com.signalcollect.evaluation

import java.io.File
import java.lang.management.GarbageCollectorMXBean
import java.lang.management.ManagementFactory
import java.util.Date
import scala.Option.option2Iterable
import scala.io.Source
import com.signalcollect.GraphBuilder
import com.signalcollect.deployment.TorqueDeployableAlgorithm
import akka.actor.ActorRef
import com.signalcollect.evaluation.resulthandling.GoogleDocsResultHandler
import collection.JavaConversions._
import com.signalcollect.Graph
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.evaluation.util.WebGraphParserGzip
import com.signalcollect.messaging.BulkMessageBus
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.GraphEditor
import com.signalcollect.interfaces.VertexToWorkerMapper
import com.signalcollect._
import java.io.FileInputStream
import java.io.BufferedReader
import java.io.FileReader
import com.signalcollect.examples.PlaceholderEdge
import scala.collection.mutable.ArrayBuffer
import com.signalcollect.evaluation.algorithms.MemoryMinimalPrecisePage
import com.signalcollect.factory.scheduler.LowLatency

object EfficientPageRankHandlers {
  def nonExistingVertex: (Edge[Int], Int) => Option[Vertex[Int, _]] = {
    (edgedId, vertexId) =>
      //Some(new MemoryMinimalPage(vertexId))
      throw new Exception(s"Vertex with id $vertexId does not exist, cannot add an edge to it.")
  }
  def undeliverableSignal: (Double, Int, Option[Int], GraphEditor[Int, Double]) => Unit = {
    case (signal, id, sourceId, ge) =>
      val v = new MemoryMinimalPrecisePage(id.asInstanceOf[Int])
      v.setTargetIdArray(Array[Int]())
      ge.addVertex(v)
      ge.sendSignal(signal, id, sourceId)
  }
}

class PageRankEvaluation extends TorqueDeployableAlgorithm {
  import EvalHelpers._

  def evaluationDescriptionKey = "evaluationDescription"
  def warmupRunsKey = "jitRepetitions"
  def datasetKey = "dataset"
  def universitiesKey = "universities"
  def spreadsheetUsernameKey = "spreadsheetUsername"
  def spreadsheetPasswordKey = "spreadsheetPassword"
  def spreadsheetNameKey = "spreadsheetName"
  def worksheetNameKey = "worksheetName"
  def graphFormatKey = "graph-format"
  def eagerIdleDetectionKey = "eager-idle-detection"
  def throttlingEnabledKey = "throttling-enabled"
  def leaderExecutionStartingTimeKey = "leaderExecutionStartingTime"

  def execute(parameters: Map[String, String], nodeActors: Array[ActorRef]) {
    println(s"Received parameters $parameters")
    val evaluationDescription = parameters(evaluationDescriptionKey)
    val warmupRuns = parameters(warmupRunsKey).toInt
    val dataset = parameters(datasetKey)
    val spreadsheetUsername = parameters(spreadsheetUsernameKey)
    val spreadsheetPassword = parameters(spreadsheetPasswordKey)
    val spreadsheetName = parameters(spreadsheetNameKey)
    val worksheetName = parameters(worksheetNameKey)
    val graphFormat = parameters(graphFormatKey)
    val eagerIdleDetectionEnabled = parameters(eagerIdleDetectionKey).toBoolean
    val throttlingEnabled = parameters(throttlingEnabledKey).toBoolean
    println(s"Creating the graph builder ...")
    val graphBuilder = (new GraphBuilder[Int, Double]).
      withPreallocatedNodes(nodeActors).
//      withSchedulerFactory(LowLatency).
//      withMessageSerialization(true).
      withEagerIdleDetection(eagerIdleDetectionEnabled).
      withThrottlingEnabled(throttlingEnabled).
      withMessageBusFactory(new BulkAkkaMessageBusFactory(100, false)).
      withAkkaMessageCompression(false).
      withHeartbeatInterval(1000)
    println(s"Building the graph")
    val g = graphBuilder.build
    try {
      println(s"Setting the undeliverable signal handler")

      def loadSplit(g: GraphEditor[Int, Double], dataset: String, splitId: Int) {
        def buildVertex(id: Int, outgoingEdges: Array[Int]): Vertex[Int, _] = {
          val vertex = new MemoryMinimalPrecisePage(id)
          vertex.setTargetIdArray(outgoingEdges)
          vertex
        }
        g.loadGraph(CompressedSplitLoader[Double](dataset, splitId, buildVertex _), Some(splitId))
      }

      println(s"Loading the graph ...")
      val loadingTime = measureTime {
        if (graphFormat != "tsv") {
          for (splitId <- 0 until 2880) { //2880
            loadSplit(g, dataset, splitId)
          }
          println(s"Awaiting idle ...")
          g.awaitIdle
        } else {
          import EfficientPageRankHandlers._
          g.setEdgeAddedToNonExistentVertexHandler {
            nonExistingVertex
          }
          g.setUndeliverableSignalHandler {
            undeliverableSignal
          }
          val datasetFileName = s"./${parameters(datasetKey)}"
          val fr = new FileReader(datasetFileName)
          val br = new BufferedReader(fr)
          val startingTime = System.currentTimeMillis
          var loadedSoFar = 0
          var currentLine = br.readLine
          val edgeBuffer = new ArrayBuffer[Int]
          var currentVertexId: Option[Int] = None
          while (currentLine != null) {
            if (loadedSoFar % 10000 == 0) {
              val millisecondsSinceLoadingStart = System.currentTimeMillis - startingTime
              val seconds = (millisecondsSinceLoadingStart / 100).round / 10
              println(s"Loaded $loadedSoFar edges after $seconds seconds.")
            }
            val split = currentLine.split("\\s+")
            val vertexId = split(0).toInt
            val targetId = split(1).toInt
            if (currentVertexId.isEmpty) {
              currentVertexId = Some(vertexId)
              edgeBuffer += targetId
            } else {
              if (currentVertexId.get == vertexId) {
                edgeBuffer += targetId
              } else {
                val v = new MemoryMinimalPrecisePage(currentVertexId.get)
                v.setTargetIdArray(edgeBuffer.toArray)
                g.addVertex(v)
                edgeBuffer.clear
                currentVertexId = Some(vertexId)
                edgeBuffer += targetId
              }
            }
            loadedSoFar += 1
            currentLine = br.readLine
          }
          if (currentVertexId.isDefined) {
            val v = new MemoryMinimalPrecisePage(currentVertexId.get)
            v.setTargetIdArray(edgeBuffer.toArray)
            g.addVertex(v)
          }
        }
        println(s"Waiting for workers to finish processing all edge additions ...")
        g.awaitIdle
        println(s"Done")
      }
      println(s"Finished loading")
      println("Starting execution ...")
      val javaVersion = ManagementFactory.getRuntimeMXBean.getVmVersion
      val jvmLibraryPath = ManagementFactory.getRuntimeMXBean.getLibraryPath
      val jvmArguments = ManagementFactory.getRuntimeMXBean.getInputArguments

      var commonResults = parameters
      commonResults += "numberOfNodes" -> g.numberOfNodes.toString
      commonResults += "numberOfWorkers" -> g.numberOfWorkers.toString
      commonResults += "java.runtime.version" -> System.getProperty("java.runtime.version")
      commonResults += (("loadingTime", loadingTime.toString))
      commonResults += (("javaVmVersion", javaVersion))
      commonResults += (("jvmLibraryPath", jvmLibraryPath))
      commonResults += (("jvmArguments", jvmArguments.mkString(" ")))
      commonResults += (("eagerIdleDetection", eagerIdleDetectionEnabled.toString))
      commonResults += (("throttling", throttlingEnabled.toString))

      val result = executeEvaluationRun(commonResults, g)
      println("All done, reporting results.")
      val leaderExecutionStartingTime = parameters(leaderExecutionStartingTimeKey).toLong
      val totalTime = System.currentTimeMillis - leaderExecutionStartingTime
      val resultReporter = new GoogleDocsResultHandler(spreadsheetUsername, spreadsheetPassword, spreadsheetName, worksheetName)
      val sumOfRanks = g.aggregate(SumOfStates[Double]).get
      resultReporter(result + ("totalTime" -> totalTime.toString) + ("sumOfRanks" -> sumOfRanks.toString))
    } finally {
      g.shutdown
    }
  }

  def executeEvaluationRun(commonResults: Map[String, String], g: Graph[Int, Double]): Map[String, String] = {
    val gcs = ManagementFactory.getGarbageCollectorMXBeans.toList
    val compilations = ManagementFactory.getCompilationMXBean
    var runResult = commonResults
    val date: Date = new Date
    val gcTimeBefore = getGcCollectionTime(gcs)
    val gcCountBefore = getGcCollectionCount(gcs)
    val compileTimeBefore = compilations.getTotalCompilationTime
    val startTime = System.nanoTime
    val stats = g.execute(ExecutionConfiguration.
      withExecutionMode(ExecutionMode.OptimizedAsynchronous).
      withSignalThreshold(0.01))
    val finishTime = System.nanoTime

    val executionTime = roundToMillisecondFraction(finishTime - startTime)
    val gcTimeAfter = getGcCollectionTime(gcs)
    val gcCountAfter = getGcCollectionCount(gcs)
    val gcTimeDuringQuery = gcTimeAfter - gcTimeBefore
    val gcCountDuringQuery = gcCountAfter - gcCountBefore
    val compileTimeAfter = compilations.getTotalCompilationTime
    val compileTimeDuringQuery = compileTimeAfter - compileTimeBefore
    runResult += (("numberOfWorkers", stats.numberOfWorkers.toString))
    runResult += (("computationTimeInMilliseconds", stats.executionStatistics.computationTime.toMillis.toString))
    runResult += (("jvmCpuTimeInMilliseconds", stats.executionStatistics.jvmCpuTime.toMillis.toString))
    runResult += (("executionTime", stats.executionStatistics.totalExecutionTime.toMillis.toString))
    runResult += (("terminationReason", stats.executionStatistics.terminationReason.toString))
    runResult += (("executionMode", stats.parameters.executionMode.toString))
    runResult += (("workerFactory", stats.config.workerFactory.toString))
    runResult += (("storageFactory", stats.config.storageFactory.toString))
    runResult += (("messageBusFactory", stats.config.messageBusFactory.toString))
    runResult += (("signalSteps", stats.executionStatistics.signalSteps.toString))
    runResult += (("collectSteps", stats.executionStatistics.collectSteps.toString))
    runResult += (("numberOfVertices", stats.aggregatedWorkerStatistics.numberOfVertices.toString))
    runResult += (("numberOfEdges", stats.aggregatedWorkerStatistics.numberOfOutgoingEdges.toString))
    runResult += (("collectOperationsExecuted", stats.aggregatedWorkerStatistics.collectOperationsExecuted.toString))
    runResult += (("signalOperationsExecuted", stats.aggregatedWorkerStatistics.signalOperationsExecuted.toString))
    runResult += (("stepsLimit", stats.parameters.stepsLimit.toString))
    runResult += (("signalThreshold", stats.parameters.signalThreshold.toString.replace('.', ',')))
    runResult += (("collectThreshold", stats.parameters.collectThreshold.toString.replace('.', ',')))
    runResult += ((s"totalRunningTime", executionTime.toString))
    runResult += ((s"totalMemory", bytesToGigabytes(Runtime.getRuntime.totalMemory).toString + "GB"))
    runResult += ((s"freeMemory", bytesToGigabytes(Runtime.getRuntime.freeMemory).toString + "GB"))
    runResult += ((s"usedMemory", bytesToGigabytes(Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory).toString + "GB"))
    runResult += ((s"executionHostname", java.net.InetAddress.getLocalHost.getHostName))
    runResult += (("gcTimeAfter", gcTimeAfter.toString))
    runResult += (("gcCountAfter", gcCountAfter.toString))
    runResult += (("gcTimeDuringQuery", gcTimeDuringQuery.toString))
    runResult += (("gcCountDuringQuery", gcCountDuringQuery.toString))
    runResult += (("compileTimeAfter", compileTimeAfter.toString))
    runResult += (("compileTimeDuringQuery", compileTimeDuringQuery.toString))
    runResult += s"date" -> date.toString
    runResult += s"system" -> "signal-collect"
    runResult
  }

}
//              if (currentVertex.id == vertexId) {
//                currentVertex.addTargetId(targetId)
//              } else {
//                g.addVertex(currentVertex)
//                currentVertex = new EfficientPageRankVertex(vertexId)
//                currentVertex.addTargetId(targetId)
//              }
//            }
//            loadedSoFar += 1
//            currentLine = br.readLine
//          }
//          if (currentVertex != null) {
//            g.addVertex(currentVertex)
//          }
