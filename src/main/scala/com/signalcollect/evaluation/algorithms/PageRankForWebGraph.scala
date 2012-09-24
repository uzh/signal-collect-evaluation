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
import com.signalcollect.graphproviders.synthetic._
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.Graph
import com.signalcollect.evaluation.algorithms._
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.graphproviders._
import com.signalcollect.evaluation.util.OptimizedGraphProvider

class PageRankForWebGraph(
  graphBuilder: GraphBuilder = GraphBuilder,
  numberOfWorkers: Int = 24,
  graphProvider: OptimizedGraphProvider,
  runConfiguration: ExecutionConfiguration = ExecutionConfiguration(ExecutionMode.PureAsynchronous).withSignalThreshold(0.01)) extends EvaluationAlgorithmRun {

  def loadGraph = {
    graph = graphBuilder.build
    graphProvider.populate(graph, 
        (id, outgoingEdges) => {
          val vertex = new MemoryMinimalPage(id.asInstanceOf[Int])
          vertex.setTargetIdArray(outgoingEdges.toArray.asInstanceOf[Array[Int]])
          vertex
        })
  }

  def execute = {
    graph.execute(runConfiguration)
  }

  def algorithmName = "PageRank"

  def graphStructure = graph.toString

}