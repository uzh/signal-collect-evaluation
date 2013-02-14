package com.signalcollect.evaluation.algorithms.paper

import com.signalcollect._

class PageRankVertex(vertexId: Any, initialState: Double)
    extends DataGraphVertex(vertexId, initialState) {
  type Signal = Double
  def collect = 0.15 + 0.85 * signals.sum
}

class PageRankEdge(targetId: Any) extends DefaultEdge(targetId) {
  type Source = PageRankVertex
  def signal = weight * source.state / source.sumOfOutWeights
}

object PageRankExample extends App {
  val graph = GraphBuilder.build
  graph.addVertex(new PageRankVertex(1, 0.15))
  graph.addVertex(new PageRankVertex(2, 0.15))
  graph.addVertex(new PageRankVertex(3, 0.15))
  graph.addEdge(1, new PageRankEdge(2))
  graph.addEdge(2, new PageRankEdge(1))
  graph.addEdge(2, new PageRankEdge(3))
  graph.addEdge(3, new PageRankEdge(2))
  graph.execute
  graph.foreachVertex(println(_))
  graph.shutdown
}
