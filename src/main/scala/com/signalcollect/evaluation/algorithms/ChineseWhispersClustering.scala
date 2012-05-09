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
package com.signalcollect.evaluation.algorithms

import com.signalcollect._
import com.signalcollect.configuration._
import com.signalcollect.graphproviders.synthetic._

/**
 * Represents an entity in a Chinese Whispers clustering algorithm.
 * Each entity determines its new state i.e. the cluster it belongs to,
 * to be the most popular cluster in among its neighbors. Ties are broken randomly.
 * Initially each entity assumes it belongs to its own cluster and therefore uses
 * its own ID as cluster label.
 */
class ChineseWhispersVertex(id: Any, selfPreference: Double = 1.0) extends DataGraphVertex(id, id) {

  type Signal = (Any, Double)

  def collect(oldState: State, mostRecentSignals: Iterable[(Any, Double)]): Any = {
    //group most recent signals by clustering label
    val grouped = (((state, selfPreference)) :: mostRecentSignals.toList).groupBy(_._1)
    //sort the grouped list by the sum of all clustering label weights
    val sorted = grouped.toList sortBy { _._2.foldLeft(0.0)((sum, elem) => sum + elem._2) }
    //return the most popular label as new state
    sorted.last._1
  }
}

/**
 * Connects two entities in a Chinese Whispers algorithm and sets the signal to be
 * the source vertex's state plus together with the weight of the connection.
 */
class ChineseWhispersEdge(s: Any, t: Any, weight: Double = 1.0) extends DefaultEdge(s, t) {
  type SourceVertex = ChineseWhispersVertex

  override def signal(sourceVertex: ChineseWhispersVertex) = (sourceVertex.state, weight)

}

/**
 * Exemplary algorithm of two fully connected clusters containing {0,1,2} and {8,9,10}
 * where nodes 2 and 8 are connected via a chain.
 */
object ChineseWhispersClustering extends App {

  //  val graph = GraphBuilder.build

//  for (i <- 0 until 11) {
//    graph.addVertex(new ChineseWhispersVertex(i))
//  }

  val loader = new LogNormalGraph(5000)

  val graph = loader.populateGraph(GraphBuilder, new ChineseWhispersVertex(_), new ChineseWhispersEdge(_, _))

  //  graph.addVertex(new ChineseWhispersVertex(1))
  //  graph.addVertex(new ChineseWhispersVertex(2))
  //  graph.addVertex(new ChineseWhispersVertex(3))
  //  graph.addVertex(new ChineseWhispersVertex(4))
  //  graph.addVertex(new ChineseWhispersVertex(5))
  //  
  //  for(i <- 0 until 2){
  //    graph.addEdge(new ChineseWhispersEdge(i, i+1))
  //    graph.addEdge(new ChineseWhispersEdge(i+1, i))
  //  }
  //  
  //  graph.addEdge(new ChineseWhispersEdge(0, 2))
  //  graph.addEdge(new ChineseWhispersEdge(2, 0))
  //
  //  for(i <- 2 until 8){
  //    graph.addEdge(new ChineseWhispersEdge(i, i+1))
  //    graph.addEdge(new ChineseWhispersEdge(i+1, i))
  //  }
  //  
  //  for(i <- 8 until 10){
  //    graph.addEdge(new ChineseWhispersEdge(i, i+1))
  //    graph.addEdge(new ChineseWhispersEdge(i+1, i))
  //  }
  //  
  //  graph.addEdge(new ChineseWhispersEdge(10, 8))
  //  graph.addEdge(new ChineseWhispersEdge(8, 10))
  //  
  val stats = graph.execute
  graph.foreachVertex(println(_))

  println(stats)

  graph.shutdown
}