package com.signalcollect.evaluation.algorithms.paper

import com.signalcollect._

class PageRankVertex(
  id: Any, initialState: Double)
  extends DataGraphVertex(id, initialState) {
  type Signal = Double
  def collect = 0.15 + 0.85 * signals.sum
}

class PageRankEdge(targetId: Any)
  extends DefaultEdge(targetId) {
  type Source = PageRankVertex
  def signal = source.state / source.edgeCount
}

object PageRankExample extends App {
  val graph = GraphBuilder.
    withStorageFactory(
      factory.storage.JavaMapStorage).
      build
  graph.addVertex(new PageRankVertex(1, 0.15))
  graph.addVertex(new PageRankVertex(2, 0.15))
  graph.addVertex(new PageRankVertex(3, 0.15))
  graph.addEdge(1, new PageRankEdge(2))
  graph.addEdge(2, new PageRankEdge(1))
  graph.addEdge(2, new PageRankEdge(3))
  graph.addEdge(3, new PageRankEdge(2))
  graph.execute(
    ExecutionConfiguration.
      withSignalThreshold(0.001))
  val top2 = graph.aggregate(
    TopKFinder[Double](k = 2))
  top2.foreach(println(_))
  graph.shutdown
}
