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
import com.signalcollect.graphproviders.GraphProvider

class GoogleGraphLoader(numberOfWorkers: Int, edgeFilename: String = "web-Google.txt", directed: Boolean = true) extends GraphProvider[Int, Float] {
  def populate(graphEditor: GraphEditor[Int, Float], vertexBuilder: (Int) => Vertex[Int, _], edgeBuilder: (Int, Int) => Edge[Int]) {
    //    //Load the vertices
    //    for (i <- (0 until numberOfWorkers).par) {
    //      graph.loadGraph(Some(i), graph => {
    //        val vertexSource = scala.io.Source.fromFile(vertexFilename)
    //        vertexSource.getLines.foreach({ line =>
    //          val vertexId = line.toInt
    //          if (vertexId % numberOfWorkers == i) {
    //            graph.addVertex(vertexBuilder(vertexId))
    //          }
    //        })
    //      })
    //    }

    //# Directed graph (each unordered pair of nodes is saved once): web-Google.txt 
    //# Webgraph from the Google programming contest, 2002
    //# Nodes: 875713 Edges: 5105039
    //# FromNodeId	ToNodeId
    //0	11342

//    graph.awaitIdle

    //Load the edges
    for (i <- (0 until numberOfWorkers).par) {
      graphEditor.modifyGraph(graph => {
        val edgeSource = scala.io.Source.fromFile(edgeFilename)
        edgeSource.getLines.foreach({ line =>
          if (!line.startsWith("#")) {
            val ids = line.split("	")
            val sourceId = ids(0).toInt
            if (sourceId % numberOfWorkers == i) {
              val targetId = ids(1).toInt
              graph.addVertex(vertexBuilder(targetId))
              graph.addVertex(vertexBuilder(sourceId))
              graph.addEdge(sourceId, edgeBuilder(sourceId, targetId))
              if (!directed) {
                graph.addEdge(targetId, edgeBuilder(targetId, sourceId))
              }
            }
          }
        })
      }, Some(i))
    }
  }

  override def toString = "GoogleFileGraphLoader" + edgeFilename

}