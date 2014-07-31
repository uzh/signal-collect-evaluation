package com.signalcollect.evaluation

import com.signalcollect.ExecutionInformation
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.Graph
import com.signalcollect.GraphBuilder
import com.signalcollect.examples.PageRankEdge
import com.signalcollect.examples.PageRankVertex
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.examples.EfficientPageRankVertex
import com.signalcollect.examples.PlaceholderEdge
import akka.event.Logging
import com.signalcollect.GraphEditor
import com.signalcollect.Vertex
import com.signalcollect.Edge
import com.signalcollect.deployment.SimpleLogger
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory

object Handlers extends com.signalcollect.logging.Logging {
  def nonExistingVertex: (Edge[Any], Any) => Option[Vertex[Any, _]] = {
    (edgedId, vertexId) => {
    	
      Some(new EfficientPageRankVertex(vertexId.asInstanceOf[Int]))
    }
  }
  def undeliverableSignal: (Any, Any, Option[Any], GraphEditor[Any, Any]) => Unit = {
    case (signal, id, sourceId, ge) =>
      ge.addVertex(new EfficientPageRankVertex(id.asInstanceOf[Int]))
      ge.sendSignal(signal, id, sourceId)
  }
}

object CirclePageRankEvaluation extends EvaluationTemplate {
  override def deploy: Boolean = true

  override def beforeStart {
    super.beforeStart
    addStat("dataset", "CirclePageRank")
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
      .withMessageBusFactory(new BulkAkkaMessageBusFactory(10000, false))
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
    //    loadOnCoordinator(graph, numberOfVertices)
    loadDistributed(graph, numberOfVertices, 1)
  }

  private def loadOnCoordinator(graph: Graph[Any, Any], numberOfVertices: Int): Graph[Any, Any] = {
    var a = 0
    for (a <- 0 to numberOfVertices - 1) {
      if (a % 1000000 == 0) {
        val runtime = Runtime.getRuntime
        val memoryUsage = 1.0 - (runtime.freeMemory() / runtime.maxMemory)
        //        log.info(s"memory used: $memoryUsage%")
        log.info(s"loaded $a vertices and edges")
      }
      graph.addEdge(a, new PlaceholderEdge((a + 1) % numberOfVertices))
    }
    graph
  }

  private def loadDistributed(graph: Graph[Any, Any], numberOfVertices: Int, splits: Int): Graph[Any, Any] = {
    val lengthOfSplit = numberOfVertices / splits
    for (id <- 0 until splits) {
      val start = id*lengthOfSplit
      val end = (id+1)*lengthOfSplit
      val loader = DistributedLoader(start, end, numberOfVertices).asInstanceOf[Iterator[GraphEditor[Any, Any] => Unit]]
      graph.loadGraph(loader, Some(id*4))
    }
    val rest = numberOfVertices -(numberOfVertices%splits)
    val loader = DistributedLoader(rest, numberOfVertices, numberOfVertices).asInstanceOf[Iterator[GraphEditor[Any, Any] => Unit]]
    graph.loadGraph(loader, Some(0))
    graph
  }
}

case class DistributedLoader(from: Int, to: Int, total: Int) extends Iterator[GraphEditor[Int, Double] => Unit] {
  var cnt = from
  def addEdge(vertices: (Int, Int))(graphEditor: GraphEditor[Int, Double]) {
    graphEditor.addEdge(vertices._2, new PlaceholderEdge[Int](vertices._1).asInstanceOf[Edge[Int]])
  }

  def hasNext = {
    cnt < to
  }

  def next: GraphEditor[Int, Double] => Unit = {
    val edge = (cnt, (cnt + 1) % total)
    if (cnt % 1000000 == 0) println(s"loaded $cnt edges")
    cnt += 1
    addEdge(edge) _
  }

}