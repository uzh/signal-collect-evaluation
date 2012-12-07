/*
 *  @author Daniel Strebel
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

import com.signalcollect.ExecutionInformation
import java.io.Serializable
import com.signalcollect.Graph

abstract class EvaluationAlgorithmRun[Id, Signal] extends Serializable {
  var graph: Graph[Id, Signal] = _
  def jvmParameters: String = ""
  def jdkBinPath: String = ""
  def loadGraph
  def buildGraph
  def execute: ExecutionInformation
  def postExecute: List[(String, String)] = List[(String, String)]()
  def shutdown: Unit = graph.shutdown
  def algorithmName: String
  def graphStructure: String
  def awaitIdle = graph.awaitIdle
  def memoryStatsEnabled: Boolean = false
}