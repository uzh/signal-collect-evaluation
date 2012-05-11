/*
 *  @author Philip Stutz
 *  
 *  Copyright 2010 University of Zurich
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

import com.signalcollect._
import scala.util.Random
import com.signalcollect.graphproviders.synthetic.Grid
import scala.collection.mutable.ArrayBuffer
import com.signalcollect.interfaces.MessageBus
import com.signalcollect.interfaces.SignalMessage
import scala.collection.mutable.HashMap
import com.signalcollect.graphproviders.synthetic.Chain

/**
 * 	This algorithm attempts to find a vertex coloring.
 * A valid vertex coloring is defined as an assignment of labels (colors)
 * 	to vertices such that no two vertices that share an edge have the same label.
 *
 * Usage restriction: this implementation *ONLY* works on *UNDIRECTED* graphs.
 * In Signal/Collect this means that there is either no edge between 2 vertices
 * or one in each direction.
 *
 * @param id: the vertex id
 * @param numColors: the number of colors (labels) used to color the graph
 */
class ColoredVertex(val id: Int, numColors: Int, var state: Int) extends Vertex {

  type Id = Int
  type State = Int
  type Signal = Int

  var lastSignalState: Int = -1

  protected var targetIdArray = Array[Int]()

  override def addOutgoingEdge(e: Edge, graphEditor: GraphEditor): Boolean = {
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

  def executeSignalOperation(messageBus: MessageBus) {
    println("signaling " + id + " signal:" + state)
    if (!targetIdArray.isEmpty) {
      val signal = state
      targetIdArray.foreach(targetId => {
        messageBus.sendToWorkerForVertexId(SignalMessage(new DefaultEdgeId(id, targetId), signal), targetId)
      })
    }
    lastSignalState = state
  }

  protected val mostRecentSignalMap = new HashMap[Int, Int]()

  override def executeCollectOperation(signals: Iterable[SignalMessage[_, _, _]], messageBus: MessageBus) {
    val castS = signals.asInstanceOf[Iterable[SignalMessage[_, Id, Signal]]]
    // faster than scala foreach
    val i = castS.iterator
    while (i.hasNext) {
      val signalMessage = i.next
      mostRecentSignalMap.put(signalMessage.edgeId.sourceId.asInstanceOf[Int], signalMessage.signal)
    }
    state = collect(state, mostRecentSignalMap.values)
  }

  def scoreCollect(signals: Iterable[SignalMessage[_, _, _]]) = signals.size

  def outgoingEdgeCount = targetIdArray.size

  def afterInitialization(graphEditor: GraphEditor) = {}
  def beforeRemoval(graphEditor: GraphEditor) = {}
  def addIncomingEdge(e: Edge, graphEditor: GraphEditor): Boolean = true
  def removeIncomingEdge(edgeId: EdgeId[_, _], graphEditor: GraphEditor): Boolean = true

  override def removeOutgoingEdge(edgeId: EdgeId[_, _], graphEditor: GraphEditor): Boolean = {
    throw new UnsupportedOperationException
  }

  override def removeAllOutgoingEdges(graphEditor: GraphEditor): Int = {
    throw new UnsupportedOperationException
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

  /** The set of available colors */
  val colors: Set[Int] = (1 to numColors).toSet

  /** Returns a random color */
  def getRandomColor: Int = Random.nextInt(numColors) + 1

  /**
   * Variable that indicates if the neighbors of this vertex should be informed
   * about its color choice. This is the case if the color has changed or if the color is the same but a conflict persists.
   */
  var informNeighbors: Boolean = false

  /**
   * Checks if one of the neighbors shares the same color. If so, the state is
   *  set to a random color and the neighbors are informed about this vertex'
   *  new color. If no neighbor shares the same color, we stay with the old color.
   */
  def collect(oldState: State, mostRecentSignals: Iterable[Int]): Int = {
    println("collecting: " + id + "received: " + mostRecentSignals.foldLeft("")(_ + ", " + _))
    if (mostRecentSignals.iterator.contains(state)) {
      println("problem!")
      informNeighbors = true
      val freeColors = colors -- mostRecentSignals
      val numberOfFreeColors = freeColors.size
      if (numberOfFreeColors > 0) {
        freeColors.toSeq(Random.nextInt(numberOfFreeColors))
      } else {
        getRandomColor
      }
    } else {
      // only inform if there was a state change since we last signaled
      informNeighbors = lastSignalState != oldState
      oldState
    }
  }

  /**
   * The signal score is 1 if this vertex hasn't signaled before or if it has
   *  changed its color (kept track of by informNeighbors). Else it's 0.
   */
  override def scoreSignal = {
    if (informNeighbors || lastSignalState == -1) {
      println("lemme signal!" + id + "last sent: " + lastSignalState)
      1
    } else {
      println("done, lastSignalState=" + lastSignalState)
      0
    }
  }

  override def toString = "ColoredVertex(id=" + id + ",state=" + state + ")"

}

/**
 * Builds a Vertex Coloring compute graph and executes the computation
 *
 * StateForwarderEdge is a built-in edge type that simply sends the state
 * of the source vertex as the signal, which means that this algorithm does
 * not require a custom edge type.
 */
object VertexColoring extends App {
  val graph = GraphBuilder.build
  //  graph.addVertex(new ColoredVertex(1, numColors = 1, state = 1))
  //  graph.addVertex(new ColoredVertex(2, numColors = 1, state = 1))
  //  graph.addVertex(new ColoredVertex(3, numColors = 1, state = 1))
  //  graph.addEdge(new StateForwarderEdge(1, 2))
  //  graph.addEdge(new StateForwarderEdge(2, 1))
  //  graph.addEdge(new StateForwarderEdge(2, 3))
  //  graph.addEdge(new StateForwarderEdge(3, 2))
  //  val grid = new Grid(100, 100)
  val edges = new Chain(3)
  for (edge <- edges) {
    graph.addVertex(new ColoredVertex(edge._1, numColors = 2, state = 1))
    graph.addVertex(new ColoredVertex(edge._2, numColors = 2, state = 1))
    graph.addEdge(new StateForwarderEdge(edge._1, edge._2))
    graph.addEdge(new StateForwarderEdge(edge._2, edge._1))
  }
  val stats = graph.execute(ExecutionConfiguration.withTimeLimit(10000))
  graph.foreachVertex(println(_))
  println(stats)
  graph.shutdown
}