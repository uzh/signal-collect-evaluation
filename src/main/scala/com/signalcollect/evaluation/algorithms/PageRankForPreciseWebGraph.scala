/*
 *  @author Daniel Strebel
 *  @author Philip Stutz
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

import scala.sys.process._
import com.signalcollect.GraphBuilder
import com.signalcollect.ExecutionInformation
import com.signalcollect.graphproviders.synthetic._
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.Graph
import com.signalcollect.evaluation.algorithms._
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.graphproviders._
import com.signalcollect.evaluation.util.OptimizedGraphProvider
import java.io.File
import org.apache.commons.io.FileUtils
import com.signalcollect.Vertex
import com.signalcollect.interfaces.AggregationOperation
import scala.annotation.tailrec
import com.signalcollect.TopKFinder

class PageRankForPreciseWebGraph(
    memoryStats: Boolean = true,
    jvmParams: String = "",
    jdkBinaryPath: String = "",
    graphBuilder: GraphBuilder[Int, Double] = new GraphBuilder[Int, Double](),
    graphProvider: OptimizedGraphProvider[Int, Double],
    numberOfWorkers: Int = 24,
    runConfiguration: ExecutionConfiguration = ExecutionConfiguration(ExecutionMode.PureAsynchronous)) extends EvaluationAlgorithmRun[Int, Double] {

  override def jvmParameters = jvmParams

  override def jdkBinPath = jdkBinaryPath

  override def memoryStatsEnabled = memoryStats

  def buildGraph {
    graph = graphBuilder.build
  }

  def loadGraph = {
    try {
      graphProvider.populate(graph,
        (id, outgoingEdges) => {
          val vertex = new MemoryMinimalPrecisePage(id)
          vertex.setTargetIdArray(outgoingEdges)
          vertex
        })
    } catch {
      case t: Throwable =>
        // Ensure that the cause and message of the exception are printed, then propagate further.
        t.printStackTrace()
        println("Message:" + t.getMessage())
        println("Cause:" + t.getCause())
        throw t
    }
  }

  def execute = {
    println("Running the algorithm ...")
    val stats = graph.execute(runConfiguration)
    println("Done.")
    stats
  }

  override def postExecute: List[(String, String)] = {
    //    val top10 = graph.aggregate(new TopKFinder[Int, Double](10, { (a: Double, b: Double) => a > b }))
    //    List(("top10Vertices", top10.toString))
    List()
  }

  def algorithmName = "PageRank"

  def graphStructure = graph.toString
}
  
  