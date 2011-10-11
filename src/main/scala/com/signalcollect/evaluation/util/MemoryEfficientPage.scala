/*
 *  @author Philip Stutz
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

package com.signalcollect.evaluation.util

import com.signalcollect.interfaces._
import com.signalcollect._
import scala.collection.mutable.ArrayBuffer
import java.io.{ ObjectInput, ObjectOutput, Externalizable }

class MemoryEfficientPage(var id: Int) extends Vertex with Externalizable {

  type Id = Int
  type State = Float
  type Signal = Float

  var state = 0.15f
  var lastSignalState: State = -1

  protected var targetIdArray = Array[Int]()

  protected var mostRecentSignalMap: Map[Int, Float] = Map[Int, Float]() // key: signal source id, value: signal

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
    0.15f + 0.85f * mostRecentSignalMap.values.foldLeft(0.0f)(_ + _)
  }

  override def executeSignalOperation(messageBus: MessageBus[Any]) {
    if (!targetIdArray.isEmpty) {
      val signal = state / targetIdArray.size
      targetIdArray.foreach(targetId => {
        messageBus.sendToWorkerForVertexId(SignalMessage(DefaultEdgeId(id, targetId), signal), targetId)
      })
    }
    lastSignalState = state
  }

  def executeCollectOperation(signals: Iterable[SignalMessage[_, _, _]], messageBus: MessageBus[Any]) {
    val castS = signals.asInstanceOf[Traversable[SignalMessage[Int, _, Signal]]]
    castS foreach { signal =>
      mostRecentSignalMap += ((signal.edgeId.sourceId, signal.signal))
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

  def afterInitialization(messageBus: MessageBus[Any]) = {}
  def beforeRemoval(messageBus: MessageBus[Any]) = {}
  def addIncomingEdge(e: Edge): Boolean = true

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
    //write most recent signals
    out.writeInt(mostRecentSignalMap.values.size)
    mostRecentSignalMap.foreach(signal => {
      out.writeInt(signal._1)
      out.writeFloat(signal._2)
    })
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

    mostRecentSignalMap = Map[Int, Float]()
    val numberOfMostRecentSignals = in.readInt
    for (i <- 0 until numberOfMostRecentSignals) {
      mostRecentSignalMap += ((in.readInt, in.readFloat))
    }

  }

  def getVertexIdsOfSuccessors: Iterable[_] = targetIdArray
  
  /**
   * Returns the ids of all vertices from which this vertex has an incoming edge, optional.
   */
  def getVertexIdsOfPredecessors: Option[Iterable[_]] = None

  /**
   * Returns the most recent signal sent via the edge with the id @edgeId. None if this function is not
   * supported or if there is no such signal.
   */
  def getMostRecentSignal(id: EdgeId[_, _]): Option[Any] = None

  override def toString = "MemoryEfficientPage (" + id + ", " + state + ")"
}

class MemoryEfficientLink(var s: Int, var t: Int) extends Edge with Externalizable {
  
  def this() = this(-1, -1)
  
  type Signal = Double
  type SourceId = Int
  type TargetId = Int
  
  def weight = 1.0
  
  def executeSignalOperation(sourceVertex: Vertex, mb: MessageBus[Any]) = {} //Since this is handled by the Page directly
  def signal(sourceVertex: SourceVertex): Signal =  0.0//Since this is handled by the Page directly
  def id = DefaultEdgeId(s, t)
  
  def writeExternal(out: ObjectOutput) {
    out.writeInt(s)
    out.writeInt(t)
  }
  
  def readExternal(in: ObjectInput) {
    s = in.readInt
    t = in.readInt
  }
  
}
