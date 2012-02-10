/*
 *  @author Daniel Strebel
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

package com.signalcollect.evaluation.graphs

import com.signalcollect._
import scala.util.Random
import scala.math._

class LogNormalGraph(graphSize: Int, seed: Long = 0, sigma: Double = 1, mu: Double = 3) extends GraphStructure {
  
  def populateGraph(builder: GraphBuilder, vertexBuilder: (Any) => Vertex, edgeBuilder: (Any, Any) => Edge) = {
    val graph = builder.build
    for (id <- (0 until graphSize).par) {
      graph.addVertex(vertexBuilder(id))
    }

    val r = new Random(seed)

    for (i <- (0 until graphSize).par) {
      val from = i
      val outDegree: Int = exp(mu + sigma * (r.nextGaussian)).round.toInt //log-normal
      var j = 0
      while (j < outDegree) {
        val to = ((r.nextDouble * (graphSize - 1))).round.toInt
        if (from != to) {
          graph.addEdge(edgeBuilder(from, to))
          j += 1
        }
      }
    }
    graph
  }
  
  override def toString = "LogNormal(" + graphSize + ", " + seed + ", " + sigma + ", " + mu + ")"
}