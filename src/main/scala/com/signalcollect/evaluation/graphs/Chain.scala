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

class Chain(graphSize: Int, symmetric: Boolean = false) extends GraphStructure {
  
    def populateGraph(builder: GraphBuilder, vertexBuilder: (Any) => Vertex, edgeBuilder: (Any, Any) => Edge) = {
      val graph = builder.build
      
      for (id <- (0 until graphSize).par) {
    	  graph.addVertex(vertexBuilder(id))
      }

      for (i <- (0 until graphSize-1)) {
        graph.addEdge(edgeBuilder(i, i+1))
      }

      graph
    }

}