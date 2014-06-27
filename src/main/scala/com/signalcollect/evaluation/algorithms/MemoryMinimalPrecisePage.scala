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
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.configuration.ExecutionMode

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

  def deliverSignal(signal: Any, sourceId: Option[Any], ge: GraphEditor[Any, Any]): Boolean = {
    val s = signal.asInstanceOf[Double]
    val newState = state + 0.85 * s
    //    if ((newState - state) < s) {
    state = newState
    //    }
    true
  }

  override def executeSignalOperation(graphEditor: GraphEditor[Any, Any]) {
    val tIds = targetIdArray
    val tIdLength = outEdges
    //    if (tIds.length != 0) {
    val signal = (state - lastSignalState) / tIdLength
    CompactIntSet.foreach(targetIdArray, graphEditor.sendSignal(signal, _, None))
    //    }
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
  val graph = GraphBuilder.
    withMessageBusFactory(new BulkAkkaMessageBusFactory(100, false)).
    withMessageSerialization(true).
    withKryoRegistrations(
      List("com.signalcollect.evaluation.algorithms.MemoryMinimalPrecisePage")).build

  val cirleVertices = 100000
  var vId = 0
  while (vId < cirleVertices) {
    val v = new MemoryMinimalPrecisePage(vId)
    val edges = Array[Int]((vId + 1) % cirleVertices)
    v.setTargetIdArray(edges)
    graph.addVertex(v)
    vId += 1
  }
  val v1 = new MemoryMinimalPrecisePage(1001)
  val edges1 = Array[Int](0, 1002)
  v1.setTargetIdArray(edges1)
  graph.addVertex(v1)

  val v2 = new MemoryMinimalPrecisePage(1002)
  val edges2 = Array[Int](1001, 0, 1, 2, 3)
  v2.setTargetIdArray(edges2)
  graph.addVertex(v2)

  val stats = graph.execute(
    ExecutionConfiguration.withSignalThreshold(0.0).
      withExecutionMode(ExecutionMode.OptimizedAsynchronous)) //(ExecutionConfiguration())
  //val stats = graph.execute //(ExecutionConfiguration())
  graph.awaitIdle
  println(stats)
  //graph.foreachVertex(println(_))
  graph.shutdown
}
