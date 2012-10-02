/*
 *  @author Daniel Strebel
 *  
 *  Copyright 2011 University of Zurich
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
import scala.collection.mutable.ArrayBuffer
import java.io.{ ObjectInput, ObjectOutput, Externalizable }

class MemoryEfficientLocation(var id: Int) extends Vertex[Int, Int] with Externalizable {

  type Signal = Int

  var state = if (id == 0) 0 else Int.MaxValue

  def setState(s: Int) {
    state = s
  }

  protected var targetIdArray = Array[Int]()
  var stateChangedSinceSignal = if (id == 0) true else false

  override def addEdge(e: Edge[_], graphEditor: GraphEditor): Boolean = {
    var edgeAdded = false
    val targetId = e.id.targetId.asInstanceOf[Int]
    if (!targetIdArray.contains(targetId)) {
      val tmp = new ArrayBuffer[Int]()
      tmp ++= targetIdArray
      tmp += targetId
      targetIdArray = tmp.toArray
      edgeAdded = true
    }
    edgeAdded
  }

  def setTargetIdArray(links: Array[Int]) = targetIdArray = links

  def deliverSignal(signal: SignalMessage[_]): Boolean = {
    val s = signal.signal.asInstanceOf[Int]
    if (s < state) {
      stateChangedSinceSignal = true
      state = s
    }
    true
  }

  def scoreCollect = 0 // because signals are directly collected at arrival

  def executeCollectOperation(graphEditor: GraphEditor) {
  }

  override def executeSignalOperation(graphEditor: GraphEditor) {
    if (!targetIdArray.isEmpty) {
      val signal = state + 1 //default weight = 1
      targetIdArray.foreach(targetId => {
        graphEditor.sendSignal(signal, EdgeId(null, targetId))
      })
    }
    stateChangedSinceSignal = false
  }

  override def scoreSignal: Double = {
    if (stateChangedSinceSignal) {
      1.0
    } else {
      0.0
    }
  }

  def scoreCollect(signals: IndexedSeq[SignalMessage[_]]) = signals.size

  def edgeCount = targetIdArray.size

  def afterInitialization(graphEditor: GraphEditor) = {}
  def beforeRemoval(graphEditor: GraphEditor) = {}
  def addIncomingEdge(e: Edge[_], graphEditor: GraphEditor): Boolean = true
  def removeIncomingEdge(edgeId: EdgeId, graphEditor: GraphEditor): Boolean = true

  override def removeEdge(targetId: Any, graphEditor: GraphEditor): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllEdges(graphEditor: GraphEditor): Int = {
    throw new UnsupportedOperationException
  }

  def getOutgoingEdgeMap: Option[Map[Any, Edge[_]]] = None
  def getOutgoingEdges: Option[Iterable[Edge[_]]] = None

  /**
   * Returns the most recent signal sent via the edge with the id @edgeId. None if this function is not
   * supported or if there is no such signal.
   */
  def getMostRecentSignal(id: EdgeId): Option[Any] = None

  override def toString = "MemoryEfficientLocation (" + id + ", " + state + ")"

  def this() = this(-1) //default constructor for serialization

  def writeExternal(out: ObjectOutput) {
    out.writeInt(id)
    out.writeInt(state)
    out.writeBoolean(stateChangedSinceSignal)
    // Write links
    out.writeInt(targetIdArray.length)
    for (i <- 0 until targetIdArray.length) {
      out.writeInt(targetIdArray(i))
    }
  }

  def readExternal(in: ObjectInput) {
    id = in.readInt
    state = in.readInt
    stateChangedSinceSignal = in.readBoolean
    //read Links
    val numberOfLinks = in.readInt
    targetIdArray = new Array[Int](numberOfLinks)
    for (i <- 0 until numberOfLinks) {
      targetIdArray(i) = in.readInt
    }
  }

}