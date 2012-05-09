/*
 *  @author Philip Stutz
 *  
 *  Copyright 2012 University of Zurich
 *      
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 */
package com.signalcollect.evaluation.util

import com.signalcollect._
import scala.util.Random
import scala.math._
import graphproviders.GraphProvider

class ParallelFileGraphLoader(numberOfWorkers: Int, vertexFilename: String, edgeFilename: String, directed: Boolean = true) extends GraphProvider {
  def populateGraph(builder: GraphBuilder, vertexBuilder: (Any) => Vertex, edgeBuilder: (Any, Any) => Edge) = {
    val graph = builder.build

    //Load the vertices
    for (i <- (0 until numberOfWorkers).par) {
      graph.loadGraph(Some(i), graph => {
        val vertexSource = scala.io.Source.fromFile(vertexFilename)
        vertexSource.getLines.foreach({ line =>
          val vertexId = line.toInt
          if (vertexId % numberOfWorkers == i) {
            graph.addVertex(vertexBuilder(vertexId))
          }
        })
      })
    }

    graph.awaitIdle

    //Load the edges
    for (i <- (0 until numberOfWorkers).par) {
      graph.loadGraph(Some(i), graph => {
        val edgeSource = scala.io.Source.fromFile(edgeFilename)
        edgeSource.getLines.foreach({ line =>
          val ids = line.split(",")
          val sourceId = ids(0).toInt
          if (sourceId % numberOfWorkers == i) {
            val targetId = ids(1).toInt
            graph.addEdge(edgeBuilder(sourceId, targetId))
            if (!directed) {
              graph.addEdge(edgeBuilder(targetId, sourceId))
            }
          }
        })
      })
    }

    graph.awaitIdle
    graph
  }

  override def toString = "ParallelFileGraphLoader" + vertexFilename + "-" + edgeFilename

}