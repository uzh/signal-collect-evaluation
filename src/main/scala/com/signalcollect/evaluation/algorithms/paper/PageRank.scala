//package com.signalcollect.evaluation.algorithms.paper
//
//import com.signalcollect.DataGraphVertex
//import com.signalcollect.DefaultEdge
//import com.signalcollect.Vertex
//import com.signalcollect.GraphBuilder
//
//class PageRankVertex(vertexId: Any, initialState: Double)
//    extends DataGraphVertex(vertexId, initialState) {
//  type Signal = Double
//  def collect(oldState: Double, signals: Iterable[Double]): Double = {
//    0.15 + 0.85 * signals.sum
//  }
//}
//
//class PageRankEdge(targetId: Any) extends DefaultEdge(targetId) {
//  def signal(source: Vertex[_, _]): Double = {
//    source match {
//      case v: PageRankVertex => weight * v.state / v.sumOfOutWeights
//    }
//  }
//}
//
//object PageRankExample extends App {
//  val graph = GraphBuilder.withConsole(true).build
//  graph.addVertex(new PageRankVertex(1, 0.15))
//  graph.addVertex(new PageRankVertex(2, 0.15))
//  graph.addVertex(new PageRankVertex(3, 0.15))
//  graph.addEdge(1, new PageRankEdge(2))
//  graph.addEdge(2, new PageRankEdge(1))
//  graph.addEdge(2, new PageRankEdge(3))
//  graph.addEdge(3, new PageRankEdge(2))
//
//  val stats = graph.execute
//  graph.awaitIdle
//  println(stats)
//  graph.foreachVertex(println(_))
//
//  graph.awaitIdle
//  readLine
//  graph.shutdown
//  
////  val l = List(1, 1, 1, 2, 3, 4, 4, 5)
////  println(l.groupBy(identity).maxBy(_._2.size)._1)
//}
