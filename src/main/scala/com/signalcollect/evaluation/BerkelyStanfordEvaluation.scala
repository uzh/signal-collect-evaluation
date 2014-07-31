package com.signalcollect.evaluation

import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.ExecutionInformation
import com.signalcollect.GraphEditor
import com.signalcollect.Graph
import com.signalcollect.GraphBuilder
import com.signalcollect.examples.PlaceholderEdge
import com.signalcollect.ExecutionConfiguration
import akka.event.Logging
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.net.URL
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory


object BerkelyStanfordEvaluation extends EvaluationTemplate {
  override def deploy: Boolean = true

  override def beforeStart {
    super.beforeStart
    addStat("dataset", "BerkleyStanford")
  }

  override def execute(g: Graph[Any, Any]): (ExecutionInformation, Graph[Any, Any]) = {
    log.info("starting execution")
    val executionMode = ExecutionMode.OptimizedAsynchronous
    addStat("executionMode", executionMode.toString)
    val stats = g.execute(ExecutionConfiguration.withExecutionMode(executionMode))
    log.info("execution finished now awaiting idle")
    (stats, g)
  }

  override def configureGraphBuilder(gb: GraphBuilder[Any, Any]): GraphBuilder[Any, Any] = {
    val eagerIdleDetection = false
    val throttling = true
    val heartbeat = 100
    addStat("eagerIdle", eagerIdleDetection.toString)
    addStat("throttling", throttling.toString)
    addStat("heartbeatInterval", heartbeat.toString)
    gb
      .withLoggingLevel(Logging.DebugLevel)
      .withEagerIdleDetection(eagerIdleDetection)
      .withThrottlingEnabled(throttling)
      .withHeartbeatInterval(heartbeat)
  }

  override def loadGraph(graph: Graph[Any, Any]): Graph[Any, Any] = {
    graph.setEdgeAddedToNonExistentVertexHandler {

      Handlers.nonExistingVertex

    }
    graph.setUndeliverableSignalHandler {
      Handlers.undeliverableSignal
    }
    graph.awaitIdle
    val numberOfVertices = 15000000
    val iterator = GzFileIterator("http://snap.stanford.edu/data/web-BerkStan.txt.gz")
    iterator.foreach(addEdge(_, graph))
    graph
  }

  def addEdge(line: String, graph: Graph[Any, Any]) {
    if (!line.contains("#")) {
      val vertices = line.split("\\s+")
      graph.addEdge(vertices(0).toInt, new PlaceholderEdge(vertices(1).toInt))
    }
  }

}

case class BufferedReaderIterator(reader: BufferedReader) extends Iterator[String] {
  override def hasNext() = reader.ready
  override def next() = reader.readLine()
}

object GzFileIterator {
  def apply(file: String) = {
    new BufferedReaderIterator(
      new BufferedReader(
        new InputStreamReader(
          new GZIPInputStream(
            new URL(file).openStream()))))
  }
}