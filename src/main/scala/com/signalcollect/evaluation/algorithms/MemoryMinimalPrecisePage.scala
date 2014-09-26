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
import scala.io.StdIn

class MemoryMinimalPrecisePage(val id: Int) extends Vertex[Int, Double, Int, Double] {

  var state = 0.15
  var pendingToSignal: Double = 0.15
  var outEdges = 0

  def targetIds: Traversable[Int] = {
    new Traversable[Int] {
      def foreach[U](f: Int => U) {
        if (outEdges != 0) {
          CompactIntSet.foreach(targetIdArray, f(_))
        }
      }
    }
  }

  def setState(s: Double) {
    state = s
  }

  protected var targetIdArray: Array[Byte] = null

  override def addEdge(e: Edge[Int], graphEditor: GraphEditor[Int, Double]): Boolean = {
    throw new UnsupportedOperationException
  }

  def setTargetIdArray(links: Array[Int]) = {
    outEdges = links.length
    targetIdArray = CompactIntSet.create(links)
  }

  override def deliverSignalWithSourceId(signal: Double, sourceId: Int, ge: GraphEditor[Int, Double]): Boolean = {
    throw new Exception("This PageRank algorithm should never receive a source ID.")
  }

  override def deliverSignalWithoutSourceId(signal: Double, ge: GraphEditor[Int, Double]): Boolean = {
    val dampedDelta = 0.85 * signal
    state += dampedDelta
    pendingToSignal += dampedDelta
    true
  }

  override def executeSignalOperation(graphEditor: GraphEditor[Int, Double]) {
    val tIds = targetIdArray
    val tIdLength = outEdges
    val signal = pendingToSignal / tIdLength
    CompactIntSet.foreach(targetIdArray, graphEditor.sendSignal(signal, _))
    pendingToSignal = 0
  }

  def executeCollectOperation(graphEditor: GraphEditor[Int, Double]) {
    throw new UnsupportedOperationException
  }

  override def scoreSignal: Double = {
    if (outEdges > 0) pendingToSignal else 0
  }

  def scoreCollect = 0 // because signals are directly collected at arrival

  def edgeCount = outEdges

  def afterInitialization(graphEditor: GraphEditor[Int, Double]) = {}
  def beforeRemoval(graphEditor: GraphEditor[Int, Double]) = {}

  override def removeEdge(targetId: Int, graphEditor: GraphEditor[Int, Double]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllEdges(graphEditor: GraphEditor[Int, Double]): Int = {
    throw new UnsupportedOperationException
  }

  override def toString = "MemoryMinimalPrecisePage(" + id + ", " + state + ")"
}

/** Builds a PageRank compute graph and executes the computation */
object MemoryMinimalPrecisePageRankTest extends App {
  val graph = new GraphBuilder[Int, Double]().
    //withStorageFactory(IntDoubleStorageFactory).
    withMessageBusFactory(new BulkAkkaMessageBusFactory[Int, Double](10000, false)).
    //withMessageSerialization(true).
    withKryoRegistrations(
      List("com.signalcollect.evaluation.algorithms.MemoryMinimalPrecisePage")).build

  val circleVertices = 10000
  var cnt = 0
  while (cnt < circleVertices) {
    val vId = cnt
    val v = new MemoryMinimalPrecisePage(vId)
    val edges = Array[Int]((vId + 1) % circleVertices)
    v.setTargetIdArray(edges)
    graph.addVertex(v)
    cnt += 1
  }
  val v1 = new MemoryMinimalPrecisePage(1001)
  val edges1 = Array[Int](0, 1002)
  v1.setTargetIdArray(edges1)
  graph.addVertex(v1)

  val v2 = new MemoryMinimalPrecisePage(1002)
  val edges2: Array[Int] = (0 to 10000).toArray
  //val edges2 = Array[Int](1001, 0, 1, 2, 3)
  v2.setTargetIdArray(edges2)
  graph.addVertex(v2)

//  println("Press any key to continue")
//  StdIn.readLine

  val stats = graph.execute(
    ExecutionConfiguration().withSignalThreshold(0.001).
      withExecutionMode(ExecutionMode.Synchronous)) //(ExecutionConfiguration())
  //val stats = graph.execute //(ExecutionConfiguration())
  graph.awaitIdle
  println(stats)
  //graph.foreachVertex(println(_))
  graph.shutdown
}
