package com.signalcollect.evaluation

import com.signalcollect.evaluation.resulthandling.GoogleDocsResultHandler
import com.signalcollect.ExecutionInformation
import scala.collection.immutable.HashMap
import com.signalcollect.Graph
import com.signalcollect.GraphBuilder
import com.signalcollect.examples.PageRankEdge
import com.signalcollect.examples.PageRankVertex
import akka.event.Logging
import com.signalcollect.deployment.Algorithm
import java.util.concurrent.TimeUnit
import com.signalcollect.deployment.DeploymentConfigurationCreator
import com.signalcollect.deployment.yarn.YarnDeploymentConfigurationCreator

abstract class EvaluationTemplate extends Algorithm {
  var startTime = 0L
  var stats: Map[String, String] = new HashMap[String, String]().asInstanceOf[Map[String, String]]
  var startLoading = 0L
  override def deploy: Boolean = true

  override def beforeStart {
    stats = new HashMap[String, String]().asInstanceOf[Map[String, String]]
    if (deploy) {
      val conf = YarnDeploymentConfigurationCreator.getYarnDeploymentConfiguration
      addStat("numberOfNodes", conf.numberOfNodes.toString)
      addStat("clusterType", conf.cluster)
      addStat("memoryPerNode", conf.memoryPerNode.toString)
      addStat("jvmArguments", conf.jvmArguments)
      if (conf.hadoopOverrides.hasPath("yarn.resourcemanager.host")) {
        addStat("host", conf.hadoopOverrides.getString("yarn.resourcemanager.host"))
      }
    }
    startTime = System.currentTimeMillis
  }

  override def afterGraphBuilt {
    startLoading = System.currentTimeMillis
  }

  override def afterGraphLoaded {
    val loadingTime = System.currentTimeMillis - startLoading
    addStat("loadingTime", loadingTime.toString)
  }

  override def reportResults(info: ExecutionInformation, graph: Graph[Any, Any]) = {
    graph.awaitIdle
    val executionTime = info.executionStatistics.computationTime.toMillis.##
    val numberOfWorkers = info.numberOfWorkers.toString
    val totalTime = System.currentTimeMillis - startTime
    val numberOfEdges = info.aggregatedWorkerStatistics.numberOfOutgoingEdges.toString
    val numberOfVertices = info.aggregatedWorkerStatistics.numberOfVertices.toString
    val handler = new GoogleDocsResultHandler("tobi.signalcollect", "s&oNY123", "yarnevaluation", "yarn")

    addStat("totalTime", totalTime.toString)
    addStat("executionTime", executionTime.toString)
    addStat("numberOfWorkers", numberOfWorkers)
    addStat("numberOfEdges", numberOfEdges)
    addStat("numberOfVertices", numberOfVertices)

    handler(stats)
  }

  def addStat(key: String, value: String) {
    stats += ((key, value))
  }

}