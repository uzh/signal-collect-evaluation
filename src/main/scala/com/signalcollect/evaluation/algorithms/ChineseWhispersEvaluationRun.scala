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
import com.signalcollect.graphproviders.GraphProvider

class ChineseWhispersEvaluationRun(
    graphBuilder: GraphBuilder[Int, Any] = new GraphBuilder[Int, Any](),
    graphProvider: GraphProvider[Int, Any],
    executionConfiguration: ExecutionConfiguration = ExecutionConfiguration(ExecutionMode.Synchronous).withSignalThreshold(0.01),
    jvmParams: String = "") extends EvaluationAlgorithmRun[Int, Any] {

  val builder: GraphBuilder[Int, Any] = graphBuilder
  var edgeTuples: Traversable[(Int, Int)] = null

  def loadGraph = {
    graph = builder.build
    graphProvider.populate(graph,
      (id) => new ChineseWhispersVertex[Int](id),
      (srcId, targetId) => {
        new ChineseWhispersEdge(srcId, targetId)
        new ChineseWhispersEdge(targetId, srcId)
      })
  }

  def execute = {
    graph.execute(executionConfiguration)
  }

  def algorithmName = "ChineseWhispers"

  def graphStructure = graphProvider.toString

  override def jvmParameters = jvmParams

}