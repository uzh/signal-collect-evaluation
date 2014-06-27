package com.signalcollect.evaluation

import java.io.File
import java.io.FileWriter
import com.signalcollect.deployment.TorqueDeployableAlgorithm
import java.util.concurrent.TimeUnit
import akka.actor.ActorRef
import akka.actor.PoisonPill
import scala.sys.process._
import com.signalcollect.evaluation.resulthandling.GoogleDocsResultHandler
import com.signalcollect.GraphBuilder

class GraphLabEval extends TorqueDeployableAlgorithm {
  def spreadsheetUsernameKey = "spreadsheetUsername"
  def spreadsheetPasswordKey = "spreadsheetPassword"
  def spreadsheetNameKey = "spreadsheetName"
  def worksheetNameKey = "worksheetName"
  def resultsFilePathKey = "results-file-path"
  def mainDirPathKey = "main-dir-path"
  def datasetKey = "dataset"
  def processesPerNodeKey = "processes-per-node"
  def numberOfNodesKey = "number-of-nodes"
  def coresKey = "cores"
  def stepsLimitKey = "steps-limit"
  def graphFormatKey = "graph-format"

  def execute(parameters: Map[String, String], nodeActors: Array[ActorRef]) {
    val g = GraphBuilder.withPreallocatedNodes(nodeActors).build
    g.shutdown // Free S/C resources.
    println("Starting GraphLab execution ...")
    assert(parameters.keySet.contains(processesPerNodeKey), s"Define the number of nodes on which to run the algorithm.")
    assert(parameters.keySet.contains(coresKey), s"Define the number of cores on which to run the algorithm.")
    assert(parameters.keySet.contains(mainDirPathKey), s"Main directory is not defined.")
    assert(parameters.keySet.contains(datasetKey), s"The dataset is not defined.")
    assert(parameters.keySet.contains(resultsFilePathKey), s"The results file path is not defined.")
    assert(parameters.keySet.contains(graphFormatKey), s"The graph format is not defined.")

    val stepsLimit = {
      if (parameters.keySet.contains(stepsLimitKey)) {
        Some(parameters(stepsLimitKey).toInt)
      } else {
        None
      }
    }

    val spreadsheetUsername = parameters(spreadsheetUsernameKey)
    val spreadsheetPassword = parameters(spreadsheetPasswordKey)
    val spreadsheetName = parameters(spreadsheetNameKey)
    val worksheetName = parameters(worksheetNameKey)

    val resultsfileName = s"${parameters(resultsFilePathKey)}/pog_${parameters(datasetKey)}_${stepsLimit match { case Some(x) => x case None => "noLimit" }}.txt"
    val datasetFileName = s"${parameters(datasetKey)}"
    val graphFormat = parameters(graphFormatKey)

    val initialString = s"mpiexec --pernode /home/user/stutz/graphlab-2.2-kraken/release/toolkits/graph_analytics/pagerank --ncpus ${parameters(coresKey)}"
    val datasetString = s" --graph $datasetFileName"
    val formatString = s" --format $graphFormat"
    //val outputString = s" --output_file $resultsfileName"
    val iterString = s" --iterations ${stepsLimit.getOrElse(0)}"
    val stdOutputString = s" 2> ${parameters(resultsFilePathKey)}stdout_${parameters(datasetKey)}_${stepsLimit match { case Some(x) => x case None => "noLimit" }}.txt"
    val finalString = initialString + datasetString + formatString + iterString + "\n"
    println("Executing: " + finalString)
    val startTime = System.currentTimeMillis
    val output = finalString.!!
    val totalTimeMs = System.currentTimeMillis - startTime
    println("Received GraphLab output:\n" + output)
    println(s"Total time: $totalTimeMs")

    val executionTimeExtractor = "Finished Running engine in ([0-9]+(\\.[0-9]*)?) seconds".r
    val executionTimeInMilliseconds: Option[Long] = executionTimeExtractor.findFirstMatchIn(output).
      map(_.group(1).toDouble).map(_ * 1000).map(_.toLong)

    val resultReporter = new GoogleDocsResultHandler(spreadsheetUsername, spreadsheetPassword, spreadsheetName, worksheetName)
    val result: Map[String, String] =
      Map("system" -> "graphlab",
        "numberOfNodes" -> parameters(numberOfNodesKey),
        "numberOfWorkers" -> (parameters(coresKey).toInt * parameters(numberOfNodesKey).toInt).toString,
        "dataset" -> parameters(datasetKey),
        "graphFormat" -> parameters(graphFormatKey),
        "stepsLimit" -> { stepsLimit match { case Some(x) => x.toString case None => "noLimit" } },
        "totalTime" -> totalTimeMs.toString,
        "executionTime" -> executionTimeInMilliseconds.getOrElse("crashed").toString)
    //"resultsFile" -> resultsfileName
    resultReporter(result)
  }
}




