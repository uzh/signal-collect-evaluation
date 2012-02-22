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
import scala.collection.mutable.ArrayBuffer
import java.io.{ ObjectInput, ObjectOutput, Externalizable }

class MemoryMinimalPage(var id: Int) extends Vertex with Externalizable {

  type Id = Int
  type State = Float
  type Signal = Float

  var state = 0.15f
  var lastSignalState: State = -1
  var othersStateSum = 0.0f

  protected var targetIdArray = Array[Int]()

  override def addOutgoingEdge(e: Edge): Boolean = {
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

  def collect: Float = {
    0.15f + 0.85f * othersStateSum
  }

  override def executeSignalOperation(messageBus: MessageBus) {
    if (!targetIdArray.isEmpty) {
      val signal = (state - Math.max(0, lastSignalState)) / targetIdArray.size
      targetIdArray.foreach(targetId => {
        messageBus.sendToWorkerForVertexId(SignalMessage(new DefaultEdgeId(id, targetId), signal), targetId)
      })
    }
    lastSignalState = state
  }

  def executeCollectOperation(signals: Iterable[SignalMessage[_, _, _]], messageBus: MessageBus) {
    val castS = signals.asInstanceOf[Traversable[SignalMessage[Int, _, Signal]]]
    castS foreach { signal =>
      othersStateSum = othersStateSum + signal.signal
    }
    state = collect
  }

  override def scoreSignal: Double = {
    if (lastSignalState >= 0) {
      (state - lastSignalState).abs
    } else {
      1
    }
  }

  def scoreCollect(signals: Iterable[SignalMessage[_, _, _]]) = signals.size

  def outgoingEdgeCount = targetIdArray.size

  def afterInitialization(messageBus: MessageBus) = {}
  def beforeRemoval(messageBus: MessageBus) = {}
  def addIncomingEdge(e: Edge): Boolean = true
  def removeIncomingEdge(edgeId: EdgeId[_, _]): Boolean = true

  override def removeOutgoingEdge(edgeId: EdgeId[_, _]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllOutgoingEdges: Int = {
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
    out.writeFloat(othersStateSum)
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

    othersStateSum = in.readFloat
    in.close

  }

  def getVertexIdsOfSuccessors: Iterable[_] = targetIdArray

  def getVertexIdsOfPredecessors: Option[Iterable[_]] = None
  def getOutgoingEdgeMap: Option[Map[EdgeId[Id, _], Edge]] = None
  def getOutgoingEdges: Option[Iterable[Edge]] = None

  /**
   * Returns the most recent signal sent via the edge with the id @edgeId. None if this function is not
   * supported or if there is no such signal.
   */
  def getMostRecentSignal(id: EdgeId[_, _]): Option[Any] = None

  override def toString = "MemoryMinimal (" + id + ", " + state + ")"
}
