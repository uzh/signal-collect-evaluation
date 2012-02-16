package com.signalcollect.evaluation.graphs

import com.signalcollect._
import scala.util.Random
import scala.math._

class DistributedLogNormal(graphSize: Int, numberOfWorkers: Option[Int] = None, seed: Long = 0, sigma: Double = 1, mu: Double = 3) extends GraphStructure {
  def populateGraph(builder: GraphBuilder, vertexBuilder: (Any) => Vertex, edgeBuilder: (Any, Any) => Edge) = {
    val graph = builder.build
    val r = new Random(seed)

    val workers = numberOfWorkers.getOrElse(24)

    //Load the vertices
    for (worker <- (0 until workers).par) {
      var vertexIdHint: Option[Int] = None
      if (numberOfWorkers.isDefined) {
        vertexIdHint = Some(worker)
      }
      for (vertexId <- worker.until(graphSize).by(workers)) {
        graph.loadGraph(vertexIdHint, graph => {
          graph.addVertex(vertexBuilder(vertexId))
        })
      }
    }

    //Load the edges
    for (worker <- (0 until workers).par) {
      var vertexIdHint: Option[Int] = None
      if (numberOfWorkers.isDefined) {
        vertexIdHint = Some(worker)
      }
      for (vertexId <- worker.until(graphSize).by(workers)) {
        graph.loadGraph(vertexIdHint, graph => {
          val outDegree: Int = exp(mu + sigma * (r.nextGaussian)).round.toInt //log-normal
          var j = 0
          while (j < outDegree) {
            val to = ((r.nextDouble * (graphSize - 1))).round.toInt
            if (vertexId != to) {
              graph.addEdge(edgeBuilder(vertexId, to))
              j += 1
            }
          }
        })
      }
    }

    graph
  }

  override def toString = "Distrubuted LogNormal(" + graphSize + ", " + numberOfWorkers + ", " + seed + ", " + sigma + ", " + mu + ")"

}