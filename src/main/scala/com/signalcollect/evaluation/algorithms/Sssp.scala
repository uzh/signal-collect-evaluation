/*
 *  @author Philip Stutz
 *  
 *  Copyright 2010 University of Zurich
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
import com.signalcollect.interfaces._
import scala.collection.mutable.IndexedSeq

/**
 * Represents an edge in a Single-Source Shortest Path compute graph.
 *  The edge weight corresponds to the length of the path represented by
 *  this edge.
 *
 *  @param s: the identifier of the source vertex
 *  @param t: the identifier of the target vertex
 */
class Path(t: Any) extends OptionalSignalEdge(t) {
  
  type Source = Location
  
  /**
   * The signal function calculates the distance of the shortest currently
   *  known path from the SSSP source vertex which passes through the source
   *  vertex of this edge. This is obviously the shortest distance to the vertex
   *  where this edge starts plus the length of the path represented by this
   *  edge (= the weight of this edge).
   */
  def signal = source.state map (_ + weight.toInt)

}

/**
 * Represents a location in a SSSP compute graph
 *
 *  @param id: the identifier of this vertex
 *  @param initialDIstance: the initial distance of this vertex to the source location
 *  if the distance is Int.MaxValue this means that there is no known path. If the distance is
 *  0 this means that this vertex is the source location.
 */
class Location(id: Any, initialState: Option[Int] = None) extends DataGraphVertex(id, initialState) {
  type Signal = Int
  /**
   * The collect function calculates the shortest currently known path
   * from the source location. This is obviously either the shortest known path
   * up to now (= state) or one of the paths that had been advertised via a signal
   * by a neighbor.
   */
  def collect: Option[Int] = {
    val currentShortestPath = state.getOrElse(Int.MaxValue)
    Some(signals.foldLeft(currentShortestPath)(math.min(_, _)))
  }

  override def scoreSignal: Double = {
    if (lastSignalState.isDefined) {
      // we signaled before
      if (lastSignalState.get.isDefined && lastSignalState.get.get != state.get) {
        // we signaled something different before, let's signal again.
        state.get - lastSignalState.get.get
      } else {
        //same thing as last time, let's not signal the same thing again.
        0
      }
    } else if (state.isDefined) {
      // we have not signaled before, but we do know a path. let's signal it then.
      1
    } else {
      // we have not signaled before and do not know a path. nothing to do here.
      0
    }
  }

}

/** Builds a Single-Source Shortest Path compute graph and executes the computation */
object SSSP extends App {
  val graph = GraphBuilder.build
  graph.addVertex(new Location(1, Some(0)))
  graph.addVertex(new Location(2))
  graph.addVertex(new Location(3))
  graph.addVertex(new Location(4))
  graph.addVertex(new Location(5))
  graph.addVertex(new Location(6))
  graph.addVertex(new Location(7))
  graph.addEdge(1, new Path(2))
  graph.addEdge(2, new Path(1))
  graph.addEdge(2, new Path(3))
  graph.addEdge(3, new Path(4))
  graph.addEdge(1, new Path(5))
  graph.addEdge(4, new Path(6))
  graph.addEdge(5, new Path(6))
  val stats = graph.execute
  println(stats)
  graph.foreachVertex(println(_))
  graph.shutdown
}
