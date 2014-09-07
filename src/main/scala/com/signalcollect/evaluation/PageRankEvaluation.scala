package com.signalcollect.evaluation

import java.io.BufferedReader
import java.lang.management.ManagementFactory
import java.util.Date
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.ArrayBuffer
import com.signalcollect.Edge
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.Graph
import com.signalcollect.GraphBuilder
import com.signalcollect.GraphEditor
import com.signalcollect.SumOfStates
import com.signalcollect.Vertex
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.deployment.TorqueDeployableAlgorithm
import com.signalcollect.evaluation.algorithms.MemoryMinimalPrecisePage
import com.signalcollect.evaluation.resulthandling.GoogleDocsResultHandler
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import EvalHelpers.bytesToGigabytes
import EvalHelpers.getGcCollectionCount
import EvalHelpers.getGcCollectionTime
import EvalHelpers.measureTime
import EvalHelpers.roundToMillisecondFraction
import akka.actor.ActorRef
import com.signalcollect.interfaces.UndeliverableSignalHandler
import com.signalcollect.interfaces.UndeliverableSignalHandlerFactory
import com.signalcollect.interfaces.EdgeAddedToNonExistentVertexHandlerFactory
import com.signalcollect.interfaces.EdgeAddedToNonExistentVertexHandler
import com.signalcollect.evaluation.algorithms.MemoryMinimalPage
import com.signalcollect.factory.messagebus.IntIdDoubleSignalMessageBusFactory
import java.io.File
import com.signalcollect.loading.Loading
import com.signalcollect.loading.VertexTupleIterator
import com.signalcollect.util.FileReader

object PrecisePageRankUndeliverableSignalHandlerFactory extends UndeliverableSignalHandlerFactory[Int, Double] {
  def createInstance: UndeliverableSignalHandler[Int, Double] = {
    PrecisePageRankUndeliverableSignalHandler
  }
}

object PrecisePageRankUndeliverableSignalHandler extends UndeliverableSignalHandler[Int, Double] {
  def vertexForSignalNotFound(signal: Double, inexistentTargetId: Int, senderId: Option[Int], graphEditor: GraphEditor[Int, Double]) {
    val v = new MemoryMinimalPrecisePage(inexistentTargetId)
    v.setTargetIdArray(Array[Int]())
    graphEditor.addVertex(v)
    graphEditor.sendSignal(signal, inexistentTargetId)
  }
}

object PrecisePageRankEdgeAddedToNonExistentVertexHandlerFactory extends EdgeAddedToNonExistentVertexHandlerFactory[Int, Double] {
  def createInstance: EdgeAddedToNonExistentVertexHandler[Int, Double] = {
    PrecisePageRankEdgeAddedToNonExistentVertexHandler
  }
}

object PrecisePageRankEdgeAddedToNonExistentVertexHandler extends EdgeAddedToNonExistentVertexHandler[Int, Double] {
  def handleImpossibleEdgeAddition(edge: Edge[Int], vertexId: Int): Option[Vertex[Int, _, Int, Double]] = {
    Some(new MemoryMinimalPrecisePage(vertexId))
    //throw new Exception(s"Vertex with id $vertexId does not exist, cannot add an edge to it.")
  }
}

object EfficientPageRankHandlers {
  def nonExistingVertex: (Edge[Int], Int) => Option[Vertex[Int, _, Int, Double]] = {
    (edgedId, vertexId) =>
      //Some(new MemoryMinimalPage(vertexId))
      throw new Exception(s"Vertex with id $vertexId does not exist, cannot add an edge to it.")
  }

  def buildVertex(id: Int, outgoingEdges: Array[Int]): Vertex[Int, _, Int, Double] = {
    val vertex = new MemoryMinimalPrecisePage(id)
    vertex.setTargetIdArray(outgoingEdges)
    vertex
  }

  def loadSplit(g: GraphEditor[Int, Double], dataset: String, splitId: Int) {
    g.loadGraph(CompressedSplitLoader[Double](dataset, splitId, buildVertex _), Some(splitId))
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
  def throttlingDuringLoadingEnabledKey = "throttling-during-loading-enabled"
  def leaderExecutionStartingTimeKey = "leaderExecutionStartingTime"
  def executionModeKey = "execution-mode"
  def hearteatIntervalKey = "heartbeat-interval"
  def bulkSizeKey = "bulksize"
  def thresholdKey = "threshold"
  def useCombinerKey = "use-combiner"

  def execute(parameters: Map[String, String], nodeActors: Array[ActorRef]) {
    println(s"Received parameters:\n${parameters.map { case (k, v) => s"\t$k = $v" }.mkString("\n")}")
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
    val throttlingDuringLoadingEnabled = parameters(throttlingDuringLoadingEnabledKey).toBoolean
    val executionMode = ExecutionMode.withName(parameters(executionModeKey))
    val heartbeatInterval = parameters(hearteatIntervalKey).toInt
    val bulksize = parameters(bulkSizeKey).toInt
    val threshold = parameters(thresholdKey).toDouble
    val useCombiner = parameters(useCombinerKey).toBoolean
    println(s"Creating the graph builder ...")
    val graphBuilder = (new GraphBuilder[Int, Double]).
      withPreallocatedNodes(nodeActors).
      //      withSchedulerFactory(LowLatency).
      //      withMessageSerialization(true).
      withEagerIdleDetection(eagerIdleDetectionEnabled).
      withThrottlingEnabled(throttlingEnabled).
      withThrottlingDuringLoadingEnabled(throttlingDuringLoadingEnabled).
      withUndeliverableSignalHandlerFactory(PrecisePageRankUndeliverableSignalHandlerFactory).
      withEdgeAddedToNonExistentVertexHandlerFactory(PrecisePageRankEdgeAddedToNonExistentVertexHandlerFactory).
      //withMessageBusFactory(new BulkAkkaMessageBusFactory(bulksize, false)).
      withMessageBusFactory(if (useCombiner) new IntIdDoubleSignalMessageBusFactory(bulksize) else new BulkAkkaMessageBusFactory(bulksize, false)).
      withHeartbeatInterval(heartbeatInterval)
    println(s"Building the graph")
    val g = graphBuilder.build
    try {
      println(s"Loading the graph ...")
      import EfficientPageRankHandlers._
      val loadingTime = measureTime {
        if (graphFormat == "binary") {
          for (splitId <- 0 until 2880) { //2880
            loadSplit(g, dataset, splitId)
          }
          println(s"Awaiting idle ...")
          g.awaitIdle
        } else if (graphFormat == "tsv") {
          // Substituting ID 0.
          val substitutingIterator = FileReader.intIterator(s"./${parameters(datasetKey)}").map { id =>
            assert(id != Int.MaxValue)
            if (id == 0) Int.MaxValue else id
          }
          val vertexData = new VertexTupleIterator(substitutingIterator)
          def vertexCreator(id: Int, edges: List[Int]) = {
            val v = new MemoryMinimalPrecisePage(id)
            v.setTargetIdArray(edges.toArray)
            v
          }
          val loader = Loading.loader(vertexData, vertexCreator)
          g.loadGraph(loader, Some(0))
        } else if (graphFormat == "adj") {
          println("Loading ADJ format")
          val dataset = new File(s"./${parameters(datasetKey)}")
          if (dataset.isDirectory) {
            for (file <- dataset.listFiles) {
              println(s"Dispatching load command for ${file.getName}")
              g.loadGraph(AdjacencyListLoader[Double](file.getAbsolutePath, EfficientPageRankHandlers.buildVertex), None)
            }
          } else {
            println(s"Dispatching load command for ${dataset.getName}")
            g.loadGraph(AdjacencyListLoader[Double](dataset.getAbsolutePath, EfficientPageRankHandlers.buildVertex), None)
          }
        } else {
          throw new Exception(s"Unrecognized graph format $graphFormat.")
        }
        println(s"Waiting for workers to finish graph loading ...")
        g.awaitIdle
        println(s"Done")
      }
      println(s"Finished loading")
      println("Starting execution ...")
      val javaVersion = ManagementFactory.getRuntimeMXBean.getVmVersion
      val jvmLibraryPath = ManagementFactory.getRuntimeMXBean.getLibraryPath
      val jvmArguments = ManagementFactory.getRuntimeMXBean.getInputArguments

      var commonResults = parameters
      commonResults += "bulksize" -> bulksize.toString
      commonResults += "useCombiner" -> useCombiner.toString
      commonResults += "heartbeatInterval" -> heartbeatInterval.toString
      commonResults += "executionMode" -> executionMode.toString
      commonResults += "numberOfNodes" -> g.numberOfNodes.toString
      commonResults += "numberOfWorkers" -> g.numberOfWorkers.toString
      commonResults += "java.runtime.version" -> System.getProperty("java.runtime.version")
      commonResults += (("loadingTime", loadingTime.toString))
      commonResults += (("javaVmVersion", javaVersion))
      commonResults += (("jvmLibraryPath", jvmLibraryPath))
      commonResults += (("jvmArguments", jvmArguments.mkString(" ")))
      commonResults += (("eagerIdleDetection", eagerIdleDetectionEnabled.toString))
      commonResults += (("throttling", throttlingEnabled.toString))
      commonResults += (("loadingThrottling", throttlingDuringLoadingEnabled.toString))

      val result = executeEvaluationRun(commonResults, threshold, executionMode, g)
      println("All done, reporting results.")
      //val leaderExecutionStartingTime = parameters(leaderExecutionStartingTimeKey).toLong
      //val totalTime = System.currentTimeMillis - leaderExecutionStartingTime
      val resultReporter = new GoogleDocsResultHandler(spreadsheetUsername, spreadsheetPassword, spreadsheetName, worksheetName)
      val sumOfRanks = g.aggregate(SumOfStates[Double]).get
      // + ("totalTime" -> totalTime.toString)
      resultReporter(result + ("sumOfRanks" -> sumOfRanks.toString))
    } finally {
      g.shutdown
    }
  }

  def executeEvaluationRun(commonResults: Map[String, String], threshold: Double, executionMode: ExecutionMode.Value, g: Graph[Int, Double]): Map[String, String] = {
    val gcs = ManagementFactory.getGarbageCollectorMXBeans.toList
    val compilations = ManagementFactory.getCompilationMXBean
    var runResult = commonResults
    val date: Date = new Date
    val gcTimeBefore = getGcCollectionTime(gcs)
    val gcCountBefore = getGcCollectionCount(gcs)
    val compileTimeBefore = compilations.getTotalCompilationTime
    val startTime = System.nanoTime
    val stats = g.execute(ExecutionConfiguration.
      withExecutionMode(executionMode).
      withSignalThreshold(threshold).
      withCollectThreshold(threshold))
    val finishTime = System.nanoTime
    println(stats)
    println(s"Individual worker statistics:\n" + stats.individualWorkerStatistics.mkString("\n"))
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
