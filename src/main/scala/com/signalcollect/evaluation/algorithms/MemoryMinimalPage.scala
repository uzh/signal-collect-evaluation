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

/**
 * Dummy for debugging loading.
 * Will never execute any computation
 */
class DummyPage(vId: Int) extends MemoryMinimalPage(vId) {
  override def executeCollectOperation(graphEditor: GraphEditor) {}
  override def scoreSignal = 0
}

class MemoryMinimalPage(var id: Int) extends Vertex[Int, Float] with Externalizable {

  var state = 0.15f
  var lastSignalState: Float = 0

  def setState(s: Float) {
    state = s
  }

  protected var targetIdArray: Array[Int] = null

  override def addEdge(e: Edge[_], graphEditor: GraphEditor): Boolean = {
    throw new UnsupportedOperationException
  }

  def setTargetIdArray(links: Array[Int]) = targetIdArray = links

  def deliverSignal(signal: SignalMessage[_]): Boolean = {
    state += 0.85f * signal.signal.asInstanceOf[Float]
    true
  }

  override def executeSignalOperation(graphEditor: GraphEditor) {
    val tIds = targetIdArray
    val tIdLength = tIds.length
    if (tIds.length != 0) {
      val signal = (state - lastSignalState) / tIdLength
      var i = 0
      while (i < tIdLength) {
        graphEditor.sendSignal(signal, EdgeId(null, tIds(i)))
        i += 1
      }
    }
    lastSignalState = state
  }

  def executeCollectOperation(graphEditor: GraphEditor) {
  }

  override def scoreSignal: Double = {
    val score = state - lastSignalState
    if (score > 0) score else 0
  }

  def scoreCollect = 0 // because signals are directly collected at arrival

  def edgeCount = targetIdArray.length

  def afterInitialization(graphEditor: GraphEditor) = {}
  def beforeRemoval(graphEditor: GraphEditor) = {}

  override def removeEdge(targetId: Any, graphEditor: GraphEditor): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllEdges(graphEditor: GraphEditor): Int = {
    throw new UnsupportedOperationException
  }

  def this() = this(-1) //default constructor for serialization

  def writeExternal(out: ObjectOutput) {
    out.writeInt(id)
    out.writeFloat(state)
    out.writeFloat(lastSignalState)
    // Write links
    out.writeInt(targetIdArray.length)
    for (i <- 0 until targetIdArray.length) {
      out.writeInt(targetIdArray(i))
    }
    //write delta buffer
    out.close
  }

  def readExternal(in: ObjectInput) {
    id = in.readInt
    state = in.readFloat
    lastSignalState = in.readFloat
    //read Links
    val numberOfLinks = in.readInt
    targetIdArray = new Array[Int](numberOfLinks)
    for (i <- 0 until numberOfLinks) {
      targetIdArray(i) = in.readInt
    }
    in.close
  }

  def getVertexIdsOfSuccessors: Iterable[_] = targetIdArray

  def getVertexIdsOfPredecessors: Option[Iterable[_]] = None
  def getOutgoingEdgeMap: Option[Map[Any, Edge[_]]] = None
  def getOutgoingEdges: Option[Iterable[Edge[_]]] = None

  override def toString = "MemoryMinimal (" + id + ", " + state + ")"
}

/** Builds a PageRank compute graph and executes the computation */
object MemoryMinimalPageRankTest extends App {
  val graph = GraphBuilder.build
  val v1 = new MemoryMinimalPage(1)
  val edges1 = new Array[Int](2)
  edges1(0) = 2
  edges1(1) = 3
  v1.setTargetIdArray(edges1)
  graph.addVertex(v1)

  val v2 = new MemoryMinimalPage(2)
  val edges2 = new Array[Int](1)
  edges1(0) = 3
  v2.setTargetIdArray(edges2)
  graph.addVertex(v2)

  val v3 = new MemoryMinimalPage(3)
  val edges3 = new Array[Int](1)
  edges1(0) = 1
  v3.setTargetIdArray(edges3)
  graph.addVertex(v3)

  val stats = graph.execute //(ExecutionConfiguration())
  graph.awaitIdle
  println(stats)
  graph.foreachVertex(println(_))
  graph.shutdown
}
