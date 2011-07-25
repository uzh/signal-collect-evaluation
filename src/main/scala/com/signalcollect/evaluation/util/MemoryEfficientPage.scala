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
import com.signalcollect.implementations.graph.MostRecentSignalsMap
import scala.collection.mutable.ArrayBuffer
import java.io.{ObjectInput, ObjectOutput, Externalizable}

class MemoryEfficientPage(var id: Int) extends Vertex[Int, Float] with Externalizable {

  var state = 0.15f
  var lastSignalState: Option[Float] = None

  type UpperSignalTypeBound = Float

  protected var targetIdArray = Array[Int]()

  protected var mostRecentSignalMap: Map[Int, Float] = Map[Int, Float]() // key: signal source id, value: signal

  override def addOutgoingEdge(e: Edge[_, _]): Boolean = {
    var edgeAdded = false
    val targetId = e.id._2.asInstanceOf[Int]
    if (!targetIdArray.contains(targetId)) {
      val tmp = new ArrayBuffer[Int]()
      tmp ++= targetIdArray
      tmp += targetId
      targetIdArray = tmp.toArray
      edgeAdded = true
    }
    edgeAdded
  }

  def collect: Float = {
    0.15f + 0.85f * mostRecentSignalMap.values.foldLeft(0.0f)(_ + _)
  }

  override def executeSignalOperation(messageBus: MessageBus[Any]) {
    if (!targetIdArray.isEmpty) {
      val signal = state / targetIdArray.size
      targetIdArray.foreach(targetId => {
        messageBus.sendToWorkerForVertexId(Signal(id, targetId, signal), targetId)
      })
    }
    lastSignalState = Some(state)
  }

  def executeCollectOperation(signals: Iterable[Signal[_, _, _]], messageBus: MessageBus[Any]) {
    val castS = signals.asInstanceOf[Traversable[Signal[Int, _, UpperSignalTypeBound]]]
    castS foreach { signal =>
    	mostRecentSignalMap += ((signal.sourceId, signal.signal))      
    }
    state = collect
  }

  override def scoreSignal: Double = {
    lastSignalState match {
      case None => 1
      case Some(oldState) => (state - oldState).abs
    }
  }

  def scoreCollect(signals: Iterable[Signal[_, _, _]]) = signals.size

  def outgoingEdgeCount = targetIdArray.size

  def afterInitialization(messageBus: MessageBus[Any]) = {}

  override def removeOutgoingEdge(edgeId: (Any, Any, String)): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllOutgoingEdges: Int = {
    throw new UnsupportedOperationException
  }
  
  def this() = this(-1) //default constructor for serialization
 
 def writeExternal(out: ObjectOutput) {
   out.writeInt(id)
   out.writeFloat(state)
   lastSignalState match {
     case Some(oldState) => out.writeFloat(oldState)
     case None => out.writeFloat(-1) //Safe because a page rank score should not be negative anyway
   }
   // Write links
   out.writeInt(targetIdArray.length)
   for(i <- 0 until targetIdArray.length) {
     out.writeInt(targetIdArray(i))
   } 
   //write most recent signals
   out.writeInt(mostRecentSignalMap.values.size)
   mostRecentSignalMap.foreach(signal => {
     out.writeInt(signal._1)
     out.writeFloat(signal._2)
   })
 }

 def readExternal(in: ObjectInput)  {
   id = in.readInt
   state = in.readFloat
   val oldSignal = in.readFloat

   if(oldSignal<0) {
     lastSignalState = None
   }
   else {
     lastSignalState = Some(oldSignal)
   }
   //read Links
   val numberOfLinks = in.readInt
   targetIdArray = new Array[Int](numberOfLinks)
   for(i <- 0 until numberOfLinks) {
     targetIdArray(i) = in.readInt
   }
   
   mostRecentSignalMap = Map[Int, Float]()
   val numberOfMostRecentSignals = in.readInt
   for(i <- 0 until numberOfMostRecentSignals) {
	   mostRecentSignalMap += ((in.readInt, in.readFloat))
   }

 }

 override def toString = "MemoryEfficientPage (" + id + ", " + state + ")"
}