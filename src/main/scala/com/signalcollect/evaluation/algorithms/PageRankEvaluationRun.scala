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

import com.signalcollect.GraphBuilder
import com.signalcollect.ExecutionInformation
import com.signalcollect.graphproviders.synthetic.LogNormal
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.Graph
import com.signalcollect.evaluation.algorithms._
import com.signalcollect.configuration.ExecutionMode

class PageRankEvaluationRun(
  graphBuilder: GraphBuilder = GraphBuilder,
  numberOfWorkers: Int = 24,
  graphSize: Int = 1000,
  executionConfiguration: ExecutionConfiguration = ExecutionConfiguration(ExecutionMode.Synchronous).withSignalThreshold(0.01)) extends EvaluationAlgorithmRun {

  val builder = graphBuilder.withNumberOfWorkers(numberOfWorkers)
  var edgeTuples: Traversable[(Int, Int)] = null

  /*
   * Synthetic graph parameters
   */
  val seed = 0
  val sigma = 1.3
  val mu = 4.0

  def loadGraph = {
    computeGraph = builder.build
    edgeTuples = new LogNormal(graphSize, seed, sigma, mu)

    for (id <- (0 until graphSize).par) {
      computeGraph.addVertex(new PageRankVertex(id, 0.15))
    }

    edgeTuples.par foreach {
      case (sourceId, targetId) =>
        computeGraph.addEdge(new PageRankEdge(sourceId, targetId))
    }

  }

  def execute = {
    computeGraph.execute(ExecutionConfiguration)
  }

  def algorithmName = "PageRank"

  def graphStructure = "LogNormal(" + graphSize + ", " + seed + ", " + sigma + ", " + mu + ")"

}