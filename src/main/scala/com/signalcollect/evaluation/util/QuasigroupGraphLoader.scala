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

//http://www.google.ch/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&ved=0CHAQFjAA&url=http%3A%2F%2Fmat.gsia.cmu.edu%2FCOLOR04%2FINSTANCES%2Fqg.order100.col&ei=Mw-tT-TXFfKK4gSe88WRDA&usg=AFQjCNHNPRbZUUvxTkkSaV7-k2dCVds44A

class QuasigroupGraphLoader(numberOfWorkers: Int, edgeFilename: String = "quasigroup100", directed: Boolean = true) extends GraphProvider[Any] {
  def populate(graphEditor: GraphEditor, vertexBuilder: (Any) => Vertex[_, _], edgeBuilder: (Any, Any) => Edge[_]) {
    for (i <- (0 until numberOfWorkers).par) {
      graphEditor.loadGraph(Some(i), ge => {
        val edgeSource = scala.io.Source.fromFile(edgeFilename)
        edgeSource.getLines.foreach({ line =>
          if (!line.startsWith("c") && !line.startsWith("p")) {
            val ids = line.split(" ")
            val sourceId = ids(1).toInt
            if (sourceId % numberOfWorkers == i) {
              val targetId = ids(2).toInt
              ge.addVertex(vertexBuilder(targetId))
              ge.addVertex(vertexBuilder(sourceId))
              ge.addEdge(sourceId, edgeBuilder(sourceId, targetId))
              if (!directed) {
                ge.addEdge(targetId, edgeBuilder(targetId, sourceId))
              }
            }
          }
        })
      })
    }
  }

  override def toString = "QuasigroupFileGraphLoader" + edgeFilename

}