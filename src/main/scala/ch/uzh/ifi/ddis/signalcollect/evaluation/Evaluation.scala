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

package ch.uzh.ifi.ddis.signalcollect.evaluation

import ch.uzh.ifi.ddis.signalcollect.api.ComputationStatistics
import ch.uzh.ifi.ddis.signalcollect.api.SynchronousComputeGraph
import ch.uzh.ifi.ddis.signalcollect.api.AsynchronousComputeGraph
import ch.uzh.ifi.ddis.signalcollect.api.ComputeGraph
import ch.uzh.ifi.ddis.signalcollect.api.Workers
import scala.concurrent.forkjoin.LinkedTransferQueue
import java.util.concurrent.LinkedBlockingQueue
import ch.uzh.ifi.ddis.signalcollect.implementations.messaging.DefaultMessageBus
import ch.uzh.ifi.ddis.signalcollect.implementations.messaging.Verbosity
import ch.uzh.ifi.ddis.signalcollect.graphproviders.synthetic.Partitions
import ch.uzh.ifi.ddis.signalcollect.graphproviders.synthetic.FullyConnected
import ch.uzh.ifi.ddis.signalcollect.graphproviders.synthetic.Star
import ch.uzh.ifi.ddis.signalcollect.algorithms.ColoredVertex
import ch.uzh.ifi.ddis.signalcollect.algorithms.Path
import ch.uzh.ifi.ddis.signalcollect.algorithms.Location
//import ch.uzh.ifi.ddis.signalcollect.algorithms.Link
//import ch.uzh.ifi.ddis.signalcollect.algorithms.Page
import ch.uzh.ifi.ddis.signalcollect.graphproviders.SparqlTuples
import ch.uzh.ifi.ddis.signalcollect.graphproviders.SesameSparql
import ch.uzh.ifi.ddis.signalcollect.graphproviders.SparqlAccessor
import ch.uzh.ifi.ddis.signalcollect.graphproviders.synthetic.LogNormal
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileWriter
import ch.uzh.ifi.ddis.signalcollect.api._
import ch.uzh.ifi.ddis.signalcollect.api.vertices._
import ch.uzh.ifi.ddis.signalcollect.api.edges._
import org.clapper.argot.{ ArgotUsageException, ArgotParser }
import org.clapper.argot.ArgotConverters._
import ch.uzh.ifi.ddis.signalcollect.implementations.messaging.MultiQueue
import java.util.concurrent.ArrayBlockingQueue
import ch.uzh.ifi.ddis.signalcollect.implementations.worker.DirectDeliveryAsynchronousWorker

/*
 * execute with command: java -Xms5000m -Xmx5000m -jar evaluation-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 */
object Evaluation {

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    // create cli (command line interface) parser
    val parser = new ArgotParser("Evaluation")

    //    val dev = parser.flag[Boolean](List("d", "dev"), "Use defaults for all options.")
    //    val input = parser.option[String](List("i", "input"), "path", "The file to read the ontology from.")(convertString)
    //    val output = parser.option[String](List("o", "output"), "path", "The file to write the results into.")(convertString)
    val workers = parser.option[Int](List("w", "workers"), "number", "The number of workers to use.")(convertInt)
    val algorithm = parser.option[String](List("a", "algorithm"), "name", "The name of the algorithm to use.")(convertString)
    val queue = parser.option[String](List("q", "queue"), "name", "The name of the queue to use.")(convertString)

    try {
      parser.parse(args) // parse command line arguments
    } catch {
      case e: ArgotUsageException => println(e.message)
    }

    val eval = new Evaluation
    if (workers.value.isDefined) {
      println("Workers: " + workers.value.get)
      eval.evaluatePagerank(List(workers.value.get))
    } else {
      eval.evaluatePagerank()
    }
  }
}

class Evaluation {

  def profilerHook = {
    println("Connect the profiler and press any key to continue:")
    val in = new InputStreamReader(System.in)
    val reader = new BufferedReader(in)
    reader.readLine
    println("Starting ...")
  }

  def evaluateLogNormalManyConnections(resultName: String, vertices: Int = 20000) {
    println("Evaluating: " + resultName)
    val et = new LogNormal(vertices)
    val gp: Map[String, Int => ComputeGraph] = Map(
      //      "Threshold Asynchronous Default Queue" -> { workers: Int => buildPingPongGraph(new AsynchronousComputeGraph(workers), et) }
      //      "Threshold Asynchronous Multi Queue" -> { workers: Int => buildPingPongGraph(new AsynchronousComputeGraphWithMultiQueue(workers), et) }
      "Threshold Synchronous" -> { workers: Int => buildPingPongGraph(new SynchronousComputeGraph(workers), et) })
    evaluate(graphProviders = gp, fileName = "fullyConnectedTopology" + resultName + ".csv", repetitions = 1, numberOfWorkers = List(1, 2, 4, 8, 64), signalThreshold = 0.001, collectThreshold = 0)
  }

  def evaluateLogNormalFewConnections(resultName: String, vertices: Int = 20000) {
    println("Evaluating: " + resultName)
    val et = new LogNormal(vertices, 0, .9, .9)
    val gp: Map[String, Int => ComputeGraph] = Map(
      "Threshold Asynchronous" -> { workers: Int => buildPingPongGraph(new AsynchronousComputeGraph(workers), et) } //      "Threshold Synchronous" -> { workers: Int => buildPingPongGraph(new SynchronousComputeGraph(workers), et) }
      )
    evaluate(graphProviders = gp, fileName = "fullyConnectedTopology" + resultName + ".csv", repetitions = 1, numberOfWorkers = List(1, 2, 3, 4, 5, 6, 7, 50, 100), signalThreshold = 0.001, collectThreshold = 0)
  }

  def evaluateFullyConnected(resultName: String, vertices: Int = 5000) {
    println("Evaluating: " + resultName)
    val et = new FullyConnected(vertices)
    val gp: Map[String, Int => ComputeGraph] = Map(
      "Threshold Asynchronous" -> { workers: Int => buildPingPongGraph(new AsynchronousComputeGraph(workers), et) },
      "Threshold Synchronous" -> { workers: Int => buildPingPongGraph(new SynchronousComputeGraph(workers), et) })
    evaluate(graphProviders = gp, fileName = "fullyConnectedTopology" + resultName + ".csv", repetitions = 1, numberOfWorkers = List(1, 2, 3, 4, 5, 6, 7, 50, 100), signalThreshold = 0.001, collectThreshold = 0)
  }

  def evaluateStar(resultName: String, vertices: Int = 100000) {
    println("Evaluating: " + resultName)
    val et = new Star(vertices, true)
    val gp: Map[String, Int => ComputeGraph] = Map(
      "Threshold Asynchronous" -> { workers: Int => buildPingPongGraph(new AsynchronousComputeGraph(workers), et) },
      "Threshold Synchronous" -> { workers: Int => buildPingPongGraph(new SynchronousComputeGraph(workers), et) })
    evaluate(graphProviders = gp, fileName = "starTopology" + resultName + ".csv", repetitions = 1, numberOfWorkers = List(1, 2, 3, 4, 5, 6, 7, 50, 100), signalThreshold = 0.001, collectThreshold = 0)
  }

  def buildPingPongGraph(cg: ComputeGraph, edgeTuples: Traversable[Tuple2[Any, Any]]): ComputeGraph = {
    edgeTuples foreach {
      case (sourceId, targetId) =>
        cg.addVertex[PingPongVertex](sourceId, 10)
        cg.addVertex[PingPongVertex](targetId, 10)
        cg.addEdge[PingPongEdge](sourceId, targetId)
    }
    cg
  }

  class PingPongVertex(id: Any, iterations: Int) extends SignalMapVertex(id, 0) {
    def collect: Int = math.min(iterations, mostRecentSignals[Int].foldLeft(state)(math.max(_, _)))
    //	  override def processResult = println(id + ": " + state)
  }

  class PingPongEdge(s: Any, t: Any) extends DefaultEdge(s, t) {
    def signal: Int = source.state.asInstanceOf[Int] + 1
  }

  def evaluatePagerank(workers: List[Int] = List(1, 2, 4, 8), repetitions: Int = 10) {

    profilerHook

    //    class MyMessageBus extends MessageBus[Any, Any] with Verbosity[Any, Any] with ProfilingMessageBus[Any, Any]
    println("Evaluating PageRank")
    //    val et = new LogNormal(1000000, 0, 1, 3)
    val et = new LogNormal(100000, 0, 1, 3)
    //    val et = new Partitions(8, 100000, 0, 1.3, 4)
    //    val et = new LogNormal(200000, 0, .9, .9)
    //            val et = new EightPartitions(100000, 10, .2, .5)
    //            val sa: SparqlAccessor = new SesameSparql("http://athena.ifi.uzh.ch:8080/openrdf-sesame", "swetodblp")
    //            val edgeQuery = "select ?source ?target { ?source <http://lsdis.cs.uga.edu/projects/semdis/opus#cites> ?target }"
    //            val et = new SparqlTuples(sa, edgeQuery)
    val gp: Map[String, Int => ComputeGraph] = Map(
      //      "Synchronous" -> { workers: Int => buildPageRankGraph(new SynchronousComputeGraph(workers), et) },
      "Direct Delivery Async" -> { workers: Int => buildPageRankGraph(new AsynchronousComputeGraph(workers, workerFactory = Workers.asynchronousDirectDeliveryWorkerFactory), et) } //      "Asynchronous" -> { workers: Int => buildPageRankGraph(new AsynchronousComputeGraph(workers, workerFactory = Workers.asynchronousWorkerFactory), et) } //    		"Linked Blocking Queue Asynchronous" -> { workers: Int => buildPageRankGraph(new AsynchronousComputeGraph(workers, messageInboxFactory = Queues.linkedBlockingQueueFactory, workerFactory = Workers.asynchronousWorkerFactory), et) },
      //    	    "Linked Blocking Queue Thread Local Direct Delivery Async" -> { workers: Int => buildThreadLocalPageRankGraph(new AsynchronousComputeGraph(workers, messageInboxFactory = Queues.linkedBlockingQueueFactory, workerFactory = Workers.asynchronousDirectDeliveryWorkerFactory), et) },
      //    		"Linked Blocking Queue Thread Local Asynchronous" -> { workers: Int => buildThreadLocalPageRankGraph(new AsynchronousComputeGraph(workers, messageInboxFactory = Queues.linkedBlockingQueueFactory, workerFactory = Workers.asynchronousWorkerFactory), et) }
      //    		"Array Blocking Queue" -> { workers: Int => buildPageRankGraph(new AsynchronousComputeGraph(workers, messageInboxFactory= { () => new ArrayBlockingQueue[Any](10000000) }), et) },
      //    		"Array Blocking Multi-Queue" -> { workers: Int => buildPageRankGraph(new AsynchronousComputeGraph(workers, messageInboxFactory= { () => new MultiQueue[Any](16, { () => new ArrayBlockingQueue[Any](1000000) }) } ), et) }
      //      "Linked Blocking Queue" -> { workers: Int => buildPageRankGraph(new AsynchronousComputeGraph(workers), et) }
      //      "Linked Blocking Multi-Queue" -> { workers: Int => buildPageRankGraph(new AsynchronousComputeGraph(workers, messageInboxFactory= { () => new MultiQueue[Any](16, { () => new LinkedBlockingQueue[Any] }) } ), et) }
      //      "Synchronous Blocking Queue" -> { workers: Int => buildPageRankGraph(new SynchronousComputeGraph(workers), et) },
      //      "NQAsynchronous Blocking Queue" -> { workers: Int => buildPageRankGraph(new NQAsynchronousComputeGraph(workers), et) }
      //      "Asynchronous Blocking Queue" -> { workers: Int => buildPageRankGraph(new AsynchronousComputeGraph(workers, messageBusFactory = { new MyMessageBus }), et) }
      )
    evaluate(graphProviders = gp, fileName = "pagerank.csv", repetitions = repetitions, numberOfWorkers = workers, signalThreshold = 0.001, collectThreshold = 0)
  }

  def buildPageRankGraph(cg: ComputeGraph, edgeTuples: Traversable[Tuple2[Int, Int]]): ComputeGraph = {
    edgeTuples foreach {
      case (sourceId, targetId) =>
        cg.addVertex[EfficientPage](sourceId, 0.85)
        cg.addVertex[EfficientPage](targetId, 0.85)
        cg.addEdge[EfficientLink](sourceId, targetId)
    }
    cg
  }

  //  def buildPageRankGraph(cg: ComputeGraph, edgeTuples: Traversable[Tuple2[Any, Any]]): ComputeGraph = {
  //    edgeTuples foreach {
  //      case (sourceId, targetId) =>
  //        cg.addVertex[Page](sourceId, 0.85)
  //        cg.addVertex[Page](targetId, 0.85)
  //        cg.addEdge[Link](sourceId, targetId)
  //    }
  //    cg
  //  }

  def evaluateSssp {
    println("Evaluating SSSP")
    //    val et = new ErdosRenyi(100000, 4, 0)
    val et = new LogNormal(5000, 0, 2.6, 0) // used for eval 1
    //    val et = new EightPartitions(10000000, 10, .3, 1)
    val gp: Map[String, Int => ComputeGraph] = Map(
      //      "Synchronous" -> { workers: Int => buildSsspGraph(new SynchronousComputeGraph(workers), et) },
      //      "AsynchronousAA" -> { workers: Int => buildSsspGraph(new AsynchronousAboveAverageComputeGraph(workers), et) },
      "Eager Asynchronous" -> { workers: Int => buildSsspGraph(new AsynchronousComputeGraph(workers), et) })
    evaluate(graphProviders = gp, fileName = "sssp.csv", repetitions = 4, numberOfWorkers = List(1, 2, 3, 4, 5, 6, 7, 8), signalThreshold = 0.001, collectThreshold = 0)
  }

  val ssspZero = 0

  def buildSsspGraph(cg: ComputeGraph, edgeTuples: Traversable[Tuple2[Any, Any]]): ComputeGraph = {
    edgeTuples foreach {
      case (sourceId, targetId) =>
        if (sourceId.equals(ssspZero))
          cg.addVertex[Location](sourceId.toString, 0)
        else
          cg.addVertex[Location](sourceId.toString, Int.MaxValue)
        cg.addVertex[Location](targetId.toString, Int.MaxValue)
        cg.addEdge[Path](sourceId.toString, targetId.toString)
    }
    cg
  }

  def evaluateGraphColoring {
    println("Evaluating Graph Coloring")
    val et = new LogNormal(100000, 10, 0.2, 1)
    //    val et = new EightPartitions(100000, 10, .2, 1)
    //	  val et = new Chain(10)
    //    val et = List((1,2), (2,3), (3,4),(1,3))
    val gp: Map[String, Int => ComputeGraph] = Map(
      "Threshold Asynchronous (\"Eager\" Scheduling)" -> { workers: Int => buildGraphColoringGraph(new AsynchronousComputeGraph(workers), et) } //      "Score-guided Synchronous" -> { workers: Int => buildGraphColoringGraph(new SynchronousComputeGraph(workers), et) }
      )
    evaluate(graphProviders = gp, fileName = "coloring.csv", repetitions = 4, numberOfWorkers = List(1, 2, 4, 8), signalThreshold = 0.001, collectThreshold = 0)
  }

  val numColors = 9 // works in 8 seconds with 15

  def buildGraphColoringGraph(cg: ComputeGraph, edgeTuples: Traversable[Tuple2[Any, Any]]): ComputeGraph = {
    edgeTuples foreach {
      case (sourceId, targetId) =>
        cg.addVertex[ColoredVertex](sourceId, numColors)
        cg.addVertex[ColoredVertex](targetId, numColors)
        cg.addEdge[StateForwarderEdge](sourceId, targetId)
        cg.addEdge[StateForwarderEdge](targetId, sourceId)
    }
    cg
  }

  val sep = ","
  val newLine = "\n"
  val tab = "\t"

  def evaluate(graphProviders: Map[String, Int => ComputeGraph], fileName: String = "eval.cvs", repetitions: Int = 1, numberOfWorkers: Traversable[Int] = List(8), signalThreshold: Double = 0.001, collectThreshold: Double = 0) {
    val fw = new FileWriter(fileName)
    try {
      var computationStatistics = Map[String, List[ComputationStatistics]]()
      for (workers <- numberOfWorkers) {
        for (graphName <- graphProviders.keys) {
          println(graphName)
          println("Building graph...")
          val computeGraph = graphProviders(graphName).apply(workers)
          println("Executing warmup...")
          println(computeGraph.execute(signalThreshold, collectThreshold)) // warmup
          computeGraph.shutDown
          var stats = List[ComputationStatistics]()
          for (i <- 1 to repetitions) {
            println
            println("GC ...")
            System.gc
            println
            println("Building graph...")
            val computeGraph = graphProviders(graphName).apply(workers)
            println("Executing...")
            val newStat = computeGraph.execute(signalThreshold, collectThreshold)
            computeGraph.shutDown
            println(newStat)
            stats = newStat :: stats
          }
          if (computationStatistics.contains(graphName))
            computationStatistics += ((graphName, computationStatistics(graphName) ::: stats))
          else
            computationStatistics += ((graphName, stats))
        }
        saveStats(computationStatistics, graphProviders.keys, workers)
        fw.flush
      }
      analyzeSpeedup(computationStatistics, graphProviders.keys)
    } finally {
      fw.close
    }

    def analyzeSpeedup(computationStatistics: Map[String, List[ComputationStatistics]], evalNames: Iterable[String]) {
      for (evalName <- evalNames) {
        println(evalName)
        val statsForEval = computationStatistics.get(evalName).get
        val t_1: Double = round(averageTimeWithXWorkers(statsForEval, 1), 2)
        println("Workers" + tab + "Time" + tab + "Speedup" + tab + "S/W") // speedup_w = t_1/t_w, w/s = speedup/workers
        println(1 + tab + t_1 + tab + 1.0 + tab + 1.0)
        if (t_1 > 0) {
          for (workers <- 2 to 1000) {
            val t_w: Double = round(averageTimeWithXWorkers(statsForEval, workers), 2)
            if (t_w > 0) {
              val speedup_w: Double = round(t_1 / t_w, 2)
              println(workers + tab + t_w + tab + speedup_w + tab + round(speedup_w / workers, 2))
            }
          }
        }
      }
    }

    def round(number: Double, decimals: Int): Double = {
      val factor = math.pow(10, decimals)
      (number * factor + .5).toInt / factor
    }

    def averageTimeWithXWorkers(l: List[ComputationStatistics], x: Int) = avg(l filter (_.numberOfWorkers.get == x) map (_.computationTimeInMilliseconds.get))

    def saveStats(computationStatistics: Map[String, List[ComputationStatistics]], evalNames: Iterable[String], workers: Int) = {
      fw.write("=========================================" + newLine)
      fw.write("Data: " + computationStatistics.mkString(sep) + newLine)
      fw.write("Workers: " + workers + newLine)
      fw.write("Average Computation Time (ms)" + sep + "Average" + sep + "DiffUp" + sep + "DiffDown" + newLine)
      for (evalName <- evalNames) {
        val values: List[Long] = computationStatistics(evalName) filter (_.numberOfWorkers.get == workers) map (_.computationTimeInMilliseconds.get)
        val stats = calculateStats(values)
        writeStats(evalName, stats._1, stats._2, stats._3, values)
      }
      fw.write(newLine)
      fw.write("Signal Operations Executed" + sep + "Average" + sep + "DiffUp" + sep + "DiffDown" + newLine)
      for (evalName <- evalNames) {
        val values: List[Long] = computationStatistics(evalName) filter (_.numberOfWorkers.get == workers) map (_.vertexSignalOperations.get)
        val stats = calculateStats(values)
        writeStats(evalName, stats._1, stats._2, stats._3, values)
      }
      fw.write(newLine)
      fw.write("Collect Operations Executed" + sep + "Average" + sep + "DiffUp" + sep + "DiffDown" + newLine)
      for (evalName <- evalNames) {
        val values: List[Long] = computationStatistics(evalName) filter (_.numberOfWorkers.get == workers) map (_.vertexCollectOperations.get)
        val stats = calculateStats(values)
        writeStats(evalName, stats._1, stats._2, stats._3, values)
      }
      fw.write("=========================================" + newLine)
      fw.write(newLine)

      def writeStats(name: String, avg: String, diffUp: String, diffDown: String, values: List[_]) {
        fw.write(name + sep + avg + sep + diffUp + sep + diffDown + sep + sep + "Values:" + sep + values.mkString(sep) + newLine)
      }
    }
  }

  def calculateStats(l: Traversable[Long]): (String, String, String) = {
    val diffUp = max(l) - avg(l)
    val diffDown = avg(l) - min(l)
    (avg(l).toString, diffUp.toString, diffDown.toString)
  }

  def avg(l: Traversable[Long]) = if (l.size > 0) l.foldLeft(0l)(_ + _) / l.size else 0
  def min(l: Traversable[Long]) = l.foldLeft(Long.MaxValue)(math.min(_, _))
  def max(l: Traversable[Long]) = l.foldLeft(Long.MinValue)(math.max(_, _))

}

