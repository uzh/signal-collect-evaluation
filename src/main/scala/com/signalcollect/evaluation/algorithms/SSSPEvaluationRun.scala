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

import com.signalcollect._
import com.signalcollect.configuration._
import com.signalcollect.graphproviders.synthetic.LogNormal
import com.signalcollect.evaluation.util.VertexFactory
import com.signalcollect.evaluation.util.LogNormalParameters
import com.signalcollect.evaluation.graphs.GraphStructure

class SSSPEvaluationRun(
  graphBuilder: GraphBuilder = GraphBuilder,
  numberOfWorkers: Int = 24,
  graph: GraphStructure,
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
    computeGraph = graph.populateGraph(builder, 
        (id) => {
        	if(id != 0) {
        	  new Location(id)
        	}
        	else {
        	  new Location(id, Some(0))
        	}
    },
    (source, target) => new Path(source, target))
  }

  def execute = {
    computeGraph.execute(ExecutionConfiguration)
  }

  def algorithmName = "SSSP"

  def graphStructure = graph.toString
}