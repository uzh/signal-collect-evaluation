package com.signalcollect.evaluation.util

import com.signalcollect.interfaces._
import com.signalcollect.implementations.graph.MostRecentSignalsMap
import scala.collection.mutable.ArrayBuffer

class MemoryEfficientPage(val id: Int) extends Vertex[Int, Float] {

  var state = 0.15f
  var lastSignalState: Option[Float] = None

  type UpperSignalTypeBound = Float

  protected var targetIdArray = Array[Int]()

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

  protected var mostRecentSignalMap: Map[Int, Float] = Map[Int, Float]() // key: signal source id, value: signal

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
    signals.foreach { signal =>
      val castS = signal.asInstanceOf[Signal[Int, _, UpperSignalTypeBound]]
      mostRecentSignalMap += ((castS.sourceId, castS.signal))
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

}