package com.signalcollect.evaluation.util

import com.signalcollect.examples.Page
import com.signalcollect.api.SignalMapVertex
import scala.collection.mutable.HashSet
import com.signalcollect.interfaces._

class MinimalisticPage(id: Int, dampingFactor: Double = 0.85) extends Page(id, dampingFactor) {
  
  protected var links = HashSet[Int]()
  
  /**
   * Adds a new outgoing {@link Edge} to this {@link FrameworkVertex} and shreds the Edge to save memory.
   * @param e the edge to be added.
   */
  override def addOutgoingEdge(e: Edge[_, _]): Boolean = {
    val newEdge = e.asInstanceOf[Edge[Int, _]]
    if (!links.contains(newEdge.id._1.asInstanceOf[Int])) {
      outgoingEdgeAddedSinceSignalOperation = true
      links.add(e.id._2.asInstanceOf[Int])
      newEdge.onAttach(this.asInstanceOf[newEdge.SourceVertexType])
      sumOfOutWeights = sumOfOutWeights + 1 //Use default weight of 1 for edges
      true
    } else {
      false
    }
  }
  
  /**
   * Removes an outgoing {@link Edge} from this {@link FrameworkVertex}.
   * @param e the edge to be added.
   */
  override def removeOutgoingEdge(edgeId: (Any, Any, String)): Boolean = {
    val castEdgeId = edgeId.asInstanceOf[(Int, Any, String)]
    val success = links.remove(castEdgeId._1)
    if(success) {
      sumOfOutWeights = sumOfOutWeights - 1 //Use default weight of 1 fore edges
    }
    success
  }
  
  /**
   * Removes all outgoing {@link Edge}s from this {@link Vertex}.
   * @return returns the number of {@link Edge}s that were removed.
   */
  override def removeAllOutgoingEdges: Int = {
    val edgesRemoved = links.size
    links.clear
    edgesRemoved
  }
  
  /**
   * Vertices directly send signals to the responsible worker of their neighbours via the messageBus
   */
  override def doSignal {
    val signal = computeSignal
    links.foreach(linkedPageID => {
    messageBus.sendToWorkerForVertexIdHash(Signal(id, linkedPageID, signal), linkedPageID.hashCode)
    })
  }
  
  /**
   * Computes the signal that is sent out to the connected pages.
   */
  def computeSignal: Double = state / sumOfOutWeights 
}