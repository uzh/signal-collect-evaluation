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
import com.signalcollect.util.Benchmark
import com.signalcollect.evaluation.resulthandling.GoogleDocsResultHandler
import collection.JavaConversions._
import com.signalcollect.Graph
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.evaluation.util.WebGraphParserGzip
import com.signalcollect.evaluation.algorithms.MemoryMinimalPage
import com.signalcollect.messaging.BulkMessageBus
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.GraphEditor
import com.signalcollect.interfaces.VertexToWorkerMapper

case object CreateUponUndeliverable {
  def handle(signal: Float, targetId: Int, sourceId: Option[Int], graphEditor: GraphEditor[Int, Float]) {
    //    val v = new MemoryMinimalPage(targetId)
    //    v.setTargetIdArray(Array())
    //    graphEditor.addVertex(v)
    //    graphEditor.sendSignal(signal, targetId, sourceId)
  }
}

class WebGraphEvaluation extends TorqueDeployableAlgorithm {
  import EvalHelpers._
  import Benchmark._

  val evaluationDescriptionKey = "evaluationDescription"
  val warmupRunsKey = "jitRepetitions"
  val datasetKey = "dataset"
  val universitiesKey = "universities"
  val spreadsheetUsernameKey = "spreadsheetUsername"
  val spreadsheetPasswordKey = "spreadsheetPassword"
  val spreadsheetNameKey = "spreadsheetName"
  val worksheetNameKey = "worksheetName"

  def execute(parameters: Map[String, String], nodeActors: Array[ActorRef]) {
    println(s"Received parameters $parameters")
    val evaluationDescription = parameters(evaluationDescriptionKey)
    val warmupRuns = parameters(warmupRunsKey).toInt
    val dataset = parameters(datasetKey)
    val spreadsheetUsername = parameters(spreadsheetUsernameKey)
    val spreadsheetPassword = parameters(spreadsheetPasswordKey)
    val spreadsheetName = parameters(spreadsheetNameKey)
    val worksheetName = parameters(worksheetNameKey)
    println(s"Creating the graph builder ...")
    val graphBuilder = (new GraphBuilder[Int, Float]).
      withPreallocatedNodes(nodeActors).
      withMessageBusFactory(new BulkAkkaMessageBusFactory(10000, false)).
      withAkkaMessageCompression(false).
      withHeartbeatInterval(1000) //.
    //      withConsole(true, 8080)

    println(s"Building the graph")
    val g = graphBuilder.build
    println(s"Setting the undeliverable signal handler")
    g.setUndeliverableSignalHandler(CreateUponUndeliverable.handle _)

    var commonResults = parameters
    commonResults += "numberOfNodes" -> g.numberOfNodes.toString
    commonResults += "numberOfWorkers" -> g.numberOfWorkers.toString
    commonResults += "java.runtime.version" -> System.getProperty("java.runtime.version")

    def loadSplit(g: GraphEditor[Int, Float], dataset: String, splitId: Int) {
      def buildVertex(id: Int, outgoingEdges: Array[Int]): MemoryMinimalPage = {
        val vertex = new MemoryMinimalPage(id)
        vertex.setTargetIdArray(outgoingEdges)
        vertex
      }
      g.loadGraph(CompressedSplitLoader(dataset, splitId, buildVertex _), Some(splitId))
    }

    println(s"Loading the graph ...")
    val loadingTime = measureTime {
      for (splitId <- 0 until 2880) { //2880
        loadSplit(g, dataset, splitId)
      }
      println(s"Awaiting idle ...")
      g.awaitIdle
      println(s"Done")
    }
    commonResults += (("loadingTime", loadingTime.toString))
    println(s"Finished loading")

    //    sleepUntilGcInactiveForXSeconds(60)

    //    println("Starting warm-up...")
    //
    //    def warmup {
    //      // TODO: Warmup JVM with smaller graph.
    //      sleepUntilGcInactiveForXSeconds(60)
    //    }

    //    val warmupTime = measureTime(warmup)
    //    commonResults += s"warmupTime" -> warmupTime.toString

    println("Starting execution ...")

    val result = executeEvaluationRun(commonResults, g)
    println("All done, reporting results.")
    val resultReporter = new GoogleDocsResultHandler(spreadsheetUsername, spreadsheetPassword, spreadsheetName, worksheetName)
    resultReporter(result)
    g.shutdown
  }

  def executeEvaluationRun(commonResults: Map[String, String], g: Graph[Int, Float]): Map[String, String] = {
    val gcs = ManagementFactory.getGarbageCollectorMXBeans.toList
    val compilations = ManagementFactory.getCompilationMXBean
    val javaVersion = ManagementFactory.getRuntimeMXBean.getVmVersion
    val jvmLibraryPath = ManagementFactory.getRuntimeMXBean.getLibraryPath
    val jvmArguments = ManagementFactory.getRuntimeMXBean.getInputArguments
    var runResult = commonResults
    runResult += (("javaVmVersion", javaVersion))
    runResult += (("jvmLibraryPath", jvmLibraryPath))
    runResult += (("jvmArguments", jvmArguments.mkString(" ")))
    val date: Date = new Date
    val gcTimeBefore = getGcCollectionTime(gcs)
    val gcCountBefore = getGcCollectionCount(gcs)
    val compileTimeBefore = compilations.getTotalCompilationTime
    val startTime = System.nanoTime
    val stats = g.execute(ExecutionConfiguration.withExecutionMode(ExecutionMode.PureAsynchronous).withSignalThreshold(0.01))
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
    runResult
  }

}
