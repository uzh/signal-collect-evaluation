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

class MemoryMinimalPage(val id: Int) extends Vertex[Int, Float, Int, Float] {

  var state = 0.15f
  var lastSignalState: Float = 0
  var outEdges = 0

  def setState(s: Float) {
    state = s
  }

  protected var targetIdArray: Array[Byte] = null

  override def addEdge(e: Edge[Int], graphEditor: GraphEditor[Int, Float]): Boolean = {
    throw new UnsupportedOperationException
  }

  def setTargetIdArray(links: Array[Int]) = {
    outEdges = links.length
    targetIdArray = CompactIntSet.create(links)
  }

  override def deliverSignalWithSourceId(signal: Float, sourceId: Int, ge: GraphEditor[Int, Float]): Boolean = {
    throw new Exception("This PageRank algorithm should never receive a source ID.")
  }

  override def deliverSignalWithoutSourceId(signal: Float, ge: GraphEditor[Int, Float]): Boolean = {
    val s = signal.asInstanceOf[Float]
    val newState = (state + 0.85 * s).toFloat
    if ((newState - state) < s) {
      state = newState
    }
    true
  }

  override def executeSignalOperation(graphEditor: GraphEditor[Int, Float]) {
    val tIds = targetIdArray
    val tIdLength = outEdges
    if (tIds.length != 0) {
      val signal = (state - lastSignalState) / tIdLength
      CompactIntSet.foreach(targetIdArray, graphEditor.sendSignal(signal, _))
    }
    lastSignalState = state
  }

  def executeCollectOperation(graphEditor: GraphEditor[Int, Float]) {
  }

  override def scoreSignal: Double = {
    state.toDouble - lastSignalState.toDouble
  }

  def scoreCollect = 0 // because signals are directly collected at arrival

  def edgeCount = outEdges

  def afterInitialization(graphEditor: GraphEditor[Int, Float]) = {}
  def beforeRemoval(graphEditor: GraphEditor[Int, Float]) = {}

  override def removeEdge(targetId: Int, graphEditor: GraphEditor[Int, Float]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllEdges(graphEditor: GraphEditor[Int, Float]): Int = {
    throw new UnsupportedOperationException
  }

  override def toString = "MemoryMinimal (" + id + ", " + state + ")"
}

// Only supports unsigned integers.
object CompactIntSet {
  def create(ints: Array[Int]): Array[Byte] = {
    val sorted = ints.sorted
    var i = 0
    var previous = 0
    while (i < sorted.length) {
      val tmp = sorted(i)
      sorted(i) = sorted(i) - previous - 1
      previous = tmp
      i += 1
    }
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    i = 0
    while (i < sorted.length) {
      writeUnsignedVarInt(sorted(i), dos)
      i += 1
    }
    dos.flush
    baos.flush
    baos.toByteArray
  }

  def foreach(encoded: Array[Byte], f: Int => Unit) {
    var i = 0
    var previousInt = 0
    var currentInt = 0
    var shift = 0
    while (i < encoded.length) {
      val readByte = encoded(i)
      currentInt |= (readByte & leastSignificant7BitsMask) << shift
      shift += 7
      if ((readByte & hasAnotherByte) == 0) {
        // Next byte is no longer part of this Int.
        previousInt += currentInt + 1
        f(previousInt)
        currentInt = 0
        shift = 0
      }
      i += 1
    }
  }

  private val hasAnotherByte = Integer.parseInt("10000000", 2)
  private val leastSignificant7BitsMask = Integer.parseInt("01111111", 2)
  private val everythingButLeastSignificant7Bits = ~leastSignificant7BitsMask

  // Same as https://developers.google.com/protocol-buffers/docs/encoding
  private def writeUnsignedVarInt(i: Int, out: DataOutputStream) {
    var remainder = i
    // While this is not the last byte, write one bit to indicate if the
    // next byte is part of this number and 7 bytes of the number itself.
    while ((remainder & everythingButLeastSignificant7Bits) != 0) {
      // First bit of byte indicates that the next byte is still part of this number, if set.
      out.writeByte((remainder & leastSignificant7BitsMask) | hasAnotherByte)
      remainder >>>= 7
    }
    // Final byte.
    out.writeByte(remainder & 0x7F)
  }

}

/** Builds a PageRank compute graph and executes the computation */
object MemoryMinimalPageRankTest extends App {
  val graph = new GraphBuilder[Int, Float]().
    withMessageBusFactory(new BulkAkkaMessageBusFactory(10000, false)).
    withMessageSerialization(true).
    withKryoRegistrations(
      List("com.signalcollect.evaluation.algorithms.MemoryMinimalPage")).build
  //  val v1 = new MemoryMinimalPage(1)
  //  val edges1 = Array[Int](2,3)
  //  v1.setTargetIdArray(edges1)
  //  graph.addVertex(v1)

  val cirleVertices = 100000
  var vId = 0
  while (vId < cirleVertices) {
    val v = new MemoryMinimalPage(vId)
    val edges = Array[Int]((vId + 1) % cirleVertices)
    v.setTargetIdArray(edges)
    graph.addVertex(v)
    vId += 1
  }

  //  val v1 = new MemoryMinimalPage(1)
  //  val edges1 = Array[Int](2)
  //  v1.setTargetIdArray(edges1)
  //  graph.addVertex(v1)
  //
  //  val v2 = new MemoryMinimalPage(2)
  //  val edges2 = Array[Int](3)
  //  v2.setTargetIdArray(edges2)
  //  graph.addVertex(v2)
  //
  //  val v3 = new MemoryMinimalPage(3)
  //  val edges3 = Array[Int](4)
  //  v3.setTargetIdArray(edges3)
  //  graph.addVertex(v3)
  //
  //  val v4 = new MemoryMinimalPage(4)
  //  val edges4 = Array[Int](1)
  //  v4.setTargetIdArray(edges4)
  //  graph.addVertex(v4)

  val stats = graph.execute(
    ExecutionConfiguration.withSignalThreshold(0.0))
  graph.awaitIdle
  println(stats)
  //graph.foreachVertex(println(_))
  graph.shutdown
}
