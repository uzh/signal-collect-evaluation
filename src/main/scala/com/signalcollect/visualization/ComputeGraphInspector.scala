package com.signalcollect.visualization

import com.signalcollect.interfaces._
import java.util.LinkedList
import scala.collection.JavaConversions._
import scala.collection.JavaConversions
import com.signalcollect.configuration.ExecutionConfiguration

class ComputeGraphInspector(val cg: ComputeGraph) {

  def getNeighbors(v: Vertex): java.lang.Iterable[Vertex] = {
    val neighbors = new LinkedList[Vertex]()
    for (neighborId <- v.getVertexIdsOfNeighbors) {
      val neighbor = cg.forVertexWithId(neighborId, { v: Vertex => v })
      if (neighbor.isDefined) {
        neighbors.add(neighbor.get)
      }
    }
    neighbors
  }

  def searchVertex(vertexId: String): java.lang.Iterable[Vertex] = {
    cg.customAggregate(List[Vertex](), { (a: List[Vertex], b: List[Vertex]) =>
      a ++ b
    }, { v: Vertex =>
      if (v.id.toString.contains(vertexId))
        List(v)
      else {
        List[Vertex]()
      }
    })
  }

  def getVertexWithId(id: Object): Vertex = {
    val vertexOption = cg.forVertexWithId(id, { v: Vertex => v })
    if (vertexOption.isDefined) {
      vertexOption.get
    } else {
      null
    }
  }

  def executeComputationStep {
    cg.execute(ExecutionConfiguration(stepsLimit = Some(1)))
  }

}