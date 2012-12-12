/*
 *  @author Philip Stutz
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

import com.signalcollect.interfaces._
import com.signalcollect._
import scala.collection.mutable.IndexedSeq
import java.io.{ ObjectInput, ObjectOutput, Externalizable }
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

class MemoryMinimalPrecisePage(val id: Int) extends Vertex[Int, Double] {

  var state = 0.15
  var lastSignalState: Double = 0
  var outEdges = 0

  def setState(s: Double) {
    state = s
  }

  protected var targetIdArray: Array[Byte] = null

  override def addEdge(e: Edge[_], graphEditor: GraphEditor[Any, Any]): Boolean = {
    throw new UnsupportedOperationException
  }

  def setTargetIdArray(links: Array[Int]) = {
    outEdges = links.length
    targetIdArray = CompactIntSet.create(links)
  }

  def deliverSignal(signal: Any, sourceId: Option[Any]): Boolean = {
    val s = signal.asInstanceOf[Double]
    val newState = state + 0.85 * s
    if ((newState - state) < s) {
      state = newState
    }
    true
  }

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    val tIds = targetIdArray
    val tIdLength = outEdges
    if (tIds.length != 0) {
      val signal = (state - lastSignalState) / tIdLength
      CompactIntSet.foreach(targetIdArray, graphEditor.sendSignal(signal, _, None))
    }
    lastSignalState = state
  }

  def executeCollectOperation(graphEditor: GraphEditor[Any, Any]) {
  }

  override def scoreSignal: Double = {
    val score = state - lastSignalState
    if (score > 0 && edgeCount > 0) score else 0
  }

  def scoreCollect = 0 // because signals are directly collected at arrival

  def edgeCount = outEdges

  def afterInitialization(graphEditor: GraphEditor[Any, Any]) = {}
  def beforeRemoval(graphEditor: GraphEditor[Any, Any]) = {}

  override def removeEdge(targetId: Any, graphEditor: GraphEditor[Any, Any]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllEdges(graphEditor: GraphEditor[Any, Any]): Int = {
    throw new UnsupportedOperationException
  }

  override def toString = "MemoryMinimalPrecisePage(" + id + ", " + state + ")"
}

/** Builds a PageRank compute graph and executes the computation */
object MemoryMinimalPrecisePageRankTest extends App {
  val graph = GraphBuilder.build
  val v1 = new MemoryMinimalPrecisePage(1)
  val edges1 = new Array[Int](2)
  edges1(0) = 2
  edges1(1) = 3
  v1.setTargetIdArray(edges1)
  graph.addVertex(v1)

  val v2 = new MemoryMinimalPrecisePage(2)
  val edges2 = new Array[Int](1)
  edges2(0) = 3
  v2.setTargetIdArray(edges2)
  graph.addVertex(v2)

  val v3 = new MemoryMinimalPrecisePage(3)
  val edges3 = new Array[Int](1)
  edges3(0) = 1
  v3.setTargetIdArray(edges3)
  graph.addVertex(v3)

  val stats = graph.execute(ExecutionConfiguration.withSignalThreshold(0.0)) //(ExecutionConfiguration())
  //val stats = graph.execute //(ExecutionConfiguration())
  graph.awaitIdle
  println(stats)
  graph.foreachVertex(println(_))
  graph.shutdown
}
