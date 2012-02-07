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

package com.signalcollect.evaluation.jobsubmission

import com.signalcollect.evaluation.resulthandling._
import com.signalcollect.evaluation.jobexecution._
import com.signalcollect.evaluation.algorithms._
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.configuration.ExecutionMode

object TestEvaluationSuite extends App {
  val evaluation = new EvaluationSuiteCreator(evaluationName = "Test_Suite_Name", 
      executionHost = new KrakenHost(krakenUsername = System.getProperty("user.name"), recompileCore = false)
      )
  
//  val evaluation = new EvaluationSuiteCreator(evaluationName = "SSSP_Test")

//  evaluation.addJobForEvaluationAlgorithm(new PageRankEvaluationRun)
//  evaluation.addJobForEvaluationAlgorithm(new PageRankEvaluationRun(executionConfiguration = ExecutionConfiguration(ExecutionMode.OptimizedAsynchronous)))
  evaluation.addJobForEvaluationAlgorithm(new SSSPEvaluationRun)

  
  //evaluation.addResultHandler(() => new ConsoleResultHandler(true))
  val googleDocsAccount = args(0)
  val googleDocsPasswd = args(1)
  evaluation.addResultHandler(new GoogleDocsResultHandler(googleDocsAccount, googleDocsPasswd, "evaluation", "data"))

  evaluation.runEvaluation()
}