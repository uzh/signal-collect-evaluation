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

package signalcollect.evaluation

import signalcollect.interfaces.ComputeGraph
import signalcollect.interfaces.Logging
import signalcollect._
import signalcollect.api._
import signalcollect.interfaces._
import signalcollect.algorithms.Link
import signalcollect.algorithms.Page
import signalcollect.algorithms.Path
import signalcollect.algorithms.Location
import signalcollect.algorithms.ColoredVertex
import signalcollect.graphproviders.synthetic._
import scala.collection.mutable.HashMap
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileWriter
import signalcollect.api.vertices._
import signalcollect.api.edges._
import signalcollect.implementations.messaging.MultiQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ArrayBlockingQueue
import signalcollect.implementations.worker.DirectDeliveryAsynchronousWorker

/*
 * execute with command: java -Xms5000m -Xmx5000m -jar algorithms-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 */
object IntegrationTests {

  val computeGraphFactories: List[Int => ComputeGraph] = List(
    { workers: Int => new SynchronousComputeGraph(workers) },
    //    		   { workers: Int => new SynchronousComputeGraph(workers, logger = Some(Loggers.createDefault)) }
    //        { workers: Int => new SynchronousComputeGraph(workers, messageBusFactory = MessageBuses.verboseMessageBusFactory, logger = Some(Loggers.createDefault)) }
    { workers: Int => new AsynchronousComputeGraph(workers) },
    //    		  			{ workers: Int => new AsynchronousComputeGraph(workers, messageBusFactory = MessageBuses.verboseMessageBusFactory, logger = Some(Loggers.createDefault)) }
    //    		   { workers: Int => new AsynchronousComputeGraph(workers, messageInboxFactory= { () => new MultiQueue[Any]({ () => new LinkedBlockingQueue[Any] }) } ) },
    //    		   { workers: Int => new AsynchronousComputeGraph(workers, messageInboxFactory= { () => new MultiQueue[Any]({ () => new ArrayBlockingQueue[Any](1000) }) } ) },
    //    		   { workers: Int => new AsynchronousComputeGraph(workers, messageInboxFactory= { () => new ArrayBlockingQueue[Any](1000) }) },
    //        		   { workers: Int => new AsynchronousComputeGraph(workers, workerFactory = { (mB, qF) => new DirectDeliveryAsynchronousWorker(mB, qF) }, messageInboxFactory= { () => new ArrayBlockingQueue[Any](100000) }) }
    //		  	{ workers: Int => new AsynchronousComputeGraph(workers, workerFactory = { (mB, qF) => new DirectDeliveryAsynchronousWorker(mB, qF) }, messageBusFactory = MessageBuses.verboseMessageBusFactory, logger = Some(Loggers.createDefault)) }
    //		  	{ workers: Int => new AsynchronousComputeGraph(workers, workerFactory = { (mB, qF) => new DirectDeliveryAsynchronousWorker(mB, qF) }) }
    { workers: Int => new AsynchronousComputeGraph(workers, workerFactory = Worker.asynchronousPriorityWorkerFactory) })

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit =
    {
      //	  profilerHook

      testPagerank
      testVertexColoring
      testSssp
    }

  def profilerHook = {
    println("Connect the profiler and press any key to continue:")
    val in = new InputStreamReader(System.in)
    val reader = new BufferedReader(in)
    reader.readLine
    println("Starting ...")
  }

  def testPagerank {
    testCycle
    testStar
    testGrid

    def testCycle {
      println("========== Graph: 5-Cycle, Algorithm: PageRank ==========")
      val et1 = List((0, 1), (1, 2), (2, 3), (3, 4), (4, 0))
      val gp1: List[Int => ComputeGraph] = for (cgFactory <- computeGraphFactories) yield { workers: Int => buildPageRankGraph(cgFactory(workers), et1) }
      def verify(v: interfaces.Vertex[_, _]) {
        println("Correct: " + ((v.state.asInstanceOf[Double] - 1).abs < 0.00001) + " Id: " + v.id + " State: " + v.state)
      }
      test(graphProviders = gp1, verify _, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0, collectThreshold = 0)
    }

    def testStar {
      println("========== Graph: 5-Star, Algorithm: PageRank ==========")
      val et2 = List((0, 4), (1, 4), (2, 4), (3, 4))
      def verify(v: interfaces.Vertex[_, _]) {
        if (v.id != 4) {
          println("Correct: " + ((v.state.asInstanceOf[Double] - 0.15).abs < 0.00001) + " Id: " + v.id + " State: " + v.state)
        } else {
          println("Correct: " + ((v.state.asInstanceOf[Double] - 0.66).abs < 0.00001) + " Id: " + v.id + " State: " + v.state)
        }
      }
      val gp2: List[Int => ComputeGraph] = for (cgFactory <- computeGraphFactories) yield { workers: Int => buildPageRankGraph(cgFactory(workers), et2) }
      test(graphProviders = gp2, verify _, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0, collectThreshold = 0)
    }

    def testGrid = {
      println("========== Graph: 2*2 Symmetric Grid, Algorithm: PageRank ==========")
      val et3 = new Grid(2, 2)
      def verify(v: interfaces.Vertex[_, _]) {
        println("Correct: " + ((v.state.asInstanceOf[Double] - 1).abs < 0.00001) + " Id: " + v.id + " State: " + v.state)
      }
      val gp3: List[Int => ComputeGraph] = for (cgFactory <- computeGraphFactories) yield { workers: Int => buildPageRankGraph(cgFactory(workers), et3) }
      test(graphProviders = gp3, verify _, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0, collectThreshold = 0)
    }
  }

  def buildPageRankGraph(cg: ComputeGraph, edgeTuples: Traversable[Tuple2[Int, Int]]): ComputeGraph = {
    edgeTuples foreach {
      case (sourceId, targetId) =>
        cg.addVertex[Page](sourceId, 0.85)
        cg.addVertex[Page](targetId, 0.85)
        cg.addEdge[Link](sourceId, targetId)
    }
    cg
  }

  class VerifiedColoredVertex(id: Int, numColors: Int) extends ColoredVertex(id, numColors, 0, false) {
    // only necessary to allow access to vertex internals
    def publicMostRecentSignals: Iterable[Int] = mostRecentSignals
  }

  def testVertexColoring {
    testCycle
    testStar
    testGrid

    def testCycle {
      println("========== Graph: Symmetric 4-Cycle, Algorithm: VertexColoring ==========")
      val et1 = List((0, 1), (1, 0), (1, 2), (2, 1), (2, 3), (3, 2), (3, 0), (0, 3))
      def verify(v: interfaces.Vertex[_, _]) {
        println("Correct: " + (v match {
          case c: VerifiedColoredVertex => !c.publicMostRecentSignals.iterator.contains(c.state)
          case other => false
        }) + " Id: " + v.id + " State: " + v.state)
      }
      val gp1: List[Int => ComputeGraph] = for (cgFactory <- computeGraphFactories) yield { workers: Int => buildVerifiedVertexColoringGraph(2, cgFactory(workers), et1) }

      test(graphProviders = gp1, verify _, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0, collectThreshold = 0)
    }

    def testStar {
      println("========== Graph: Symmetric 5-Star, Algorithm: VertexColoring ==========")
      val et2 = List((0, 4), (4, 0), (1, 4), (4, 1), (2, 4), (4, 2), (3, 4), (4, 3))
      def verify(v: interfaces.Vertex[_, _]) {
        println("Correct: " + (v match {
          case c: VerifiedColoredVertex => !c.publicMostRecentSignals.iterator.contains(c.state)
          case other => false
        }) + " Id: " + v.id + " State: " + v.state)
      }
      val gp2: List[Int => ComputeGraph] = for (cgFactory <- computeGraphFactories) yield { workers: Int => buildVerifiedVertexColoringGraph(2, cgFactory(workers), et2) }
      test(graphProviders = gp2, verify _, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0, collectThreshold = 0)
    }

    def testGrid = {
      println("========== Graph: 2*2 Symmetric Grid, Algorithm: VertexColoring ==========")
      val et3 = new Grid(2, 2)
      def verify(v: interfaces.Vertex[_, _]) {
        println("Correct: " + (v match {
          case c: VerifiedColoredVertex => !c.publicMostRecentSignals.iterator.contains(c.state)
          case other => false
        }) + " Id: " + v.id + " State: " + v.state)
      }
      val gp3: List[Int => ComputeGraph] = for (cgFactory <- computeGraphFactories) yield { workers: Int => buildVerifiedVertexColoringGraph(2, cgFactory(workers), et3) }
      test(graphProviders = gp3, verify _, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0, collectThreshold = 0)
    }
  }

  def buildVerifiedVertexColoringGraph(numColors: Int, cg: ComputeGraph, edgeTuples: Traversable[Tuple2[Int, Int]]): ComputeGraph = {
    edgeTuples foreach {
      case (sourceId, targetId) =>
        cg.addVertex[VerifiedColoredVertex](sourceId.asInstanceOf[AnyRef], numColors)
        cg.addVertex[VerifiedColoredVertex](targetId.asInstanceOf[AnyRef], numColors)
        cg.addEdge[StateForwarderEdge](sourceId, targetId)
    }
    cg
  }

  def testSssp {
    testCycle
    testStar
    testGrid

    def testCycle {
      println("========== Graph: Symmetric 4-Cycle, Algorithm: SSSP ==========")
      val et1 = List((0, 1), (1, 2), (2, 3), (3, 0))
      def verify(v: interfaces.Vertex[_, _]) {
        println("Correct: " + (v.id == v.state.asInstanceOf[Option[Int]].get) + " Id: " + v.id + " State: " + v.state)
      }
      val gp1: List[Int => ComputeGraph] = for (cgFactory <- computeGraphFactories) yield { workers: Int => buildSsspGraph(0, cgFactory(workers), et1) }
      test(graphProviders = gp1, verify _, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0, collectThreshold = 0)
    }

    def testStar {
      println("========== Graph: Symmetric 5-Star, Algorithm: SSSP ==========")
      val et2 = List((0, 4), (4, 0), (1, 4), (4, 1), (2, 4), (4, 2), (3, 4), (4, 3))
      def verify(v: interfaces.Vertex[_, _]) {
        if (v.id.asInstanceOf[Int] == 4) {
          println("Correct: " + (v.state.asInstanceOf[Option[Int]].get == 0) + " Id: " + v.id + " State: " + v.state)
        } else {
          println("Correct: " + (v.state.asInstanceOf[Option[Int]].get == 1) + " Id: " + v.id + " State: " + v.state)
        }
      }
      val gp2: List[Int => ComputeGraph] = for (cgFactory <- computeGraphFactories) yield { workers: Int => buildSsspGraph(4, cgFactory(workers), et2) }
      test(graphProviders = gp2, verify, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0, collectThreshold = 0)
    }

    def testGrid = {
      println("========== Graph: 2*2 Symmetric Grid, Algorithm: SSSP ==========")
      val et3 = new Grid(2, 2)
      def verify(v: interfaces.Vertex[_, _]) {
        v.id.asInstanceOf[Int] match {
          case 1 => println("Correct: " + (v.state.asInstanceOf[Option[Int]].get == 0) + " Id: " + v.id + " State: " + v.state)
          case 2 => println("Correct: " + (v.state.asInstanceOf[Option[Int]].get == 1) + " Id: " + v.id + " State: " + v.state)
          case 3 => println("Correct: " + (v.state.asInstanceOf[Option[Int]].get == 1) + " Id: " + v.id + " State: " + v.state)
          case 4 => println("Correct: " + (v.state.asInstanceOf[Option[Int]].get == 2) + " Id: " + v.id + " State: " + v.state)
        }
      }
      val gp3: List[Int => ComputeGraph] = for (cgFactory <- computeGraphFactories) yield { workers: Int => buildSsspGraph(1, cgFactory(workers), et3) }
      test(graphProviders = gp3, verify _, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0, collectThreshold = 0)
    }
  }

  def buildSsspGraph(pathSourceId: Any, cg: ComputeGraph, edgeTuples: Traversable[Tuple2[Int, Int]]): ComputeGraph = {
    edgeTuples foreach {
      case (sourceId, targetId) =>
        if (sourceId.equals(pathSourceId)) {
          cg.addVertex[Location](sourceId.asInstanceOf[AnyRef], Some(0))
        } else {
          cg.addVertex[Location](sourceId.asInstanceOf[AnyRef], None)
        }
        if (targetId.equals(pathSourceId)) {
          cg.addVertex[Location](targetId.asInstanceOf[AnyRef], Some(0))
        } else {
          cg.addVertex[Location](targetId.asInstanceOf[AnyRef], None)
        }
        cg.addEdge[Path](sourceId, targetId)
    }
    cg
  }

  def test(graphProviders: List[Int => ComputeGraph], verify: interfaces.Vertex[_, _] => Unit, numberOfWorkers: Traversable[Int] = List(8), signalThreshold: Double = 0.001, collectThreshold: Double = 0) {
    var computationStatistics = Map[String, List[ComputationStatistics]]()
    for (workers <- numberOfWorkers) {
      for (graphProvider <- graphProviders) {
        val cg = graphProvider.apply(workers)
        //print("\tMode: " + cg)
        cg.setSignalThreshold(signalThreshold)
        cg.setCollectThreshold(collectThreshold)
        cg.execute
        println("\tWorkers: " + workers)
        val correct = cg foreach (verify(_))
        cg.shutDown
      }
    }
    println
  }

  def avg(l: Traversable[Long]) = if (l.size > 0) l.foldLeft(0l)(_ + _) / l.size else 0
  def min(l: Traversable[Long]) = l.foldLeft(Long.MaxValue)(math.min(_, _))
  def max(l: Traversable[Long]) = l.foldLeft(Long.MinValue)(math.max(_, _))

}