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

class PageRankForWebGraph(
    memoryStats: Boolean = true,
    jvmParams: String = "",
    jdkBinaryPath: String = "",
    graphBuilder: GraphBuilder = GraphBuilder,
    graphProvider: OptimizedGraphProvider,
    numberOfWorkers: Int = 24,
    runConfiguration: ExecutionConfiguration = ExecutionConfiguration(ExecutionMode.PureAsynchronous).withSignalThreshold(0.01)) extends EvaluationAlgorithmRun {

  override def jvmParameters = jvmParams

  override def jdkBinPath = jdkBinaryPath

  override def memoryStatsEnabled = memoryStats

  def loadGraph = {
    val localHostName = java.net.InetAddress.getLocalHost().getHostName()
    if (localHostName.contains("kraken") || localHostName.contains("claudio")) {
      val localFileDir = "/home/torque/tmp/webgraph-tmp"
      val remoteFileDir = "/home/user/" + System.getProperty("user.name") + "/webgraph"

      def existsLocalCopy: Boolean = {
        val f = new File(localFileDir)
        f.exists
      }

      def createDirectory {
        val f = new File(localFileDir)
        if (!f.exists) {
          f.mkdir
        }
      }

      def copyAllSplits {
        val source = new File(remoteFileDir)
        val dest = new File(localFileDir)
        FileUtils.copyDirectory(source, dest)
      }

      if (!existsLocalCopy) {
        createDirectory
        println("Copying splits to local machine ...")
        copyAllSplits
        println("Done")
      }
    }
    try {
      graph = graphBuilder.build
      graphProvider.populate(graph,
        (id, outgoingEdges) => {
          val vertex = new MemoryMinimalPage(id)
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

  def algorithmName = "PageRank"

  def graphStructure = graph.toString

}