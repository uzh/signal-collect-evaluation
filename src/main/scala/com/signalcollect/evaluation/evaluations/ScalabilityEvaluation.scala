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

package com.signalcollect.evaluation.evaluations

import com.signalcollect.evaluation.jobsubmission._
import com.signalcollect.evaluation.jobexecution._
import com.signalcollect.evaluation.resulthandling._
import com.signalcollect.evaluation.algorithms.PageRankEvaluationRun
import com.signalcollect.configuration._
import com.signalcollect._

object ScalabilityEvaluation extends App {

  val repetitions = 3
  val evaluation = new EvaluationSuiteCreator(evaluationName = "Test_Suite_Name",
    executionHost = new KrakenHost(krakenUsername = System.getProperty("user.name"), recompileCore = false))
  val googleDocsAccount = args(0)
  val googleDocsPasswd = args(1)
  evaluation.addResultHandler(new GoogleDocsResultHandler(googleDocsAccount, googleDocsPasswd, "evaluation", "data"))

  for (i <- 0 until repetitions) {
	  for (workers <- 1 to 24) {
	      evaluation.addJobForEvaluationAlgorithm(new PageRankEvaluationRun(numberOfWorkers = workers, 
	          executionConfiguration = ExecutionConfiguration(ExecutionMode.OptimizedAsynchronous).withSignalThreshold(0.01)))
	  }
  }
}