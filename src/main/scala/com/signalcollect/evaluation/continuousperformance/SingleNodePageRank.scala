package com.signalcollect.evaluation.continuousperformance

import com.signalcollect.nodeprovisioning.torque._
import com.signalcollect.evaluation.resulthandling.GoogleDocsResultHandler
import java.io._
import scala.collection.mutable.ArrayBuffer
import com.signalcollect._
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.configuration.ExecutionMode._
import com.signalcollect.examples._
import java.util.Date
import java.text.SimpleDateFormat

object SingleNodePageRank extends App {

  val jvmParameters = " -Xmx28000m" +
    " -Xms28000m"

  val kraken = new TorqueHost(
    jobSubmitter = new LocalJobSubmitter("strebel@ifi.uzh.ch"),
    localJarPath = args(2), jvmParameters = jvmParameters, priority = TorquePriority.superfast)
  val localHost = new LocalHost
  val googleDocs = new GoogleDocsResultHandler(args(0), args(1), "continuous", "data")
  val runsPerEvaluationRun = 10
  val lastCommit = if (args.size >= 4) args(3) else "unknown"
  

  var evaluation = new Evaluation(evaluationName = "continous performance evaluation", executionHost = kraken).addResultHandler(googleDocs)

  for (i <- 0 until runsPerEvaluationRun) {
    evaluation = evaluation.addEvaluationRun(runPageRank)
  }
  
  evaluation.addExtraStats(Map("commit" -> lastCommit))

  evaluation.execute

  def runPageRank(): List[Map[String, String]] = {
		  
    val lastCommit = if (args.size >= 4) args(3) else "unknown"
      
    def runEvaluation(): Map[String, String] = {

      def cleanGarbage {
        for (i <- 1 to 10) {
          System.gc
          Thread.sleep(100)
        }
        Thread.sleep(10000)
      }

      val g = new GraphBuilder[Int, Double].withMessageBusFactory(new BulkAkkaMessageBusFactory(1024, false)).build
      val numberOfSplits = Runtime.getRuntime.availableProcessors
      val splits = {
        val s = new Array[DataInputStream](numberOfSplits)
        for (i <- 0 until numberOfSplits) {
          s(i) = new DataInputStream(new FileInputStream(System.getProperty("user.home") + s"/web-split-$i"))
        }
        s
      }
      def loadSplit(splitIndex: Int)(ge: GraphEditor[Int, Double]) {
        val in = splits(splitIndex)
        var vertexId = CompactIntSet.readUnsignedVarInt(in)
        while (vertexId >= 0) {
          val numberOfEdges = CompactIntSet.readUnsignedVarInt(in)
          var edges = new ArrayBuffer[Int]
          while (edges.length < numberOfEdges) {
            val nextEdge = CompactIntSet.readUnsignedVarInt(in)
            edges += nextEdge
          }
          val vertex = new EfficientPageRankVertex(vertexId)
          vertex.setTargetIds(edges.length, CompactIntSet.create(edges.toArray))
          ge.addVertex(vertex)
          vertexId = CompactIntSet.readUnsignedVarInt(in)
        }
      }

      for (i <- 0 until numberOfSplits) {
        g.modifyGraph(loadSplit(i), Some(i))
      }
      print("Loading graph ...")
      g.awaitIdle
      println("done.")

      //make sure that all garbage is collected before starting the computation
      cleanGarbage

      print("Running computation ...")
      val stats = g.execute(ExecutionConfiguration.withExecutionMode(PureAsynchronous).withSignalThreshold(0.001))
      println("done.")

      val startDate = new Date
      val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
      val timeFormat = new SimpleDateFormat("HH:mm:ss")

      var statsMap = Map[String, String]()
      statsMap += (("startDate", dateFormat.format(startDate)))
      statsMap += (("startTime", timeFormat.format(startDate)))

      if (stats != null) {
        statsMap += (("numberOfWorkers", stats.numberOfWorkers.toString))
        statsMap += (("computationTimeInMilliseconds", stats.executionStatistics.computationTime.toMillis.toString))
        statsMap += (("jvmCpuTimeInMilliseconds", stats.executionStatistics.jvmCpuTime.toMillis.toString))
        statsMap += (("totalExecutionTimeInMilliseconds", stats.executionStatistics.totalExecutionTime.toMillis.toString))
        statsMap += (("terminationReason", stats.executionStatistics.terminationReason.toString))
        statsMap += (("executionMode", stats.parameters.executionMode.toString))
        statsMap += (("workerFactory", stats.config.workerFactory.toString))
        statsMap += (("storageFactory", stats.config.storageFactory.toString))
        statsMap += (("messageBusFactory", stats.config.messageBusFactory.toString))
        statsMap += (("logger", stats.config.logger.toString))
        statsMap += (("signalSteps", stats.executionStatistics.signalSteps.toString))
        statsMap += (("collectSteps", stats.executionStatistics.collectSteps.toString))
        statsMap += (("numberOfVertices", stats.aggregatedWorkerStatistics.numberOfVertices.toString))
        statsMap += (("numberOfEdges", stats.aggregatedWorkerStatistics.numberOfOutgoingEdges.toString))
        statsMap += (("collectOperationsExecuted", stats.aggregatedWorkerStatistics.collectOperationsExecuted.toString))
        statsMap += (("signalOperationsExecuted", stats.aggregatedWorkerStatistics.signalOperationsExecuted.toString))
        statsMap += (("stepsLimit", stats.parameters.stepsLimit.toString))
        statsMap += (("signalThreshold", stats.parameters.signalThreshold.toString.replace('.', ',')))
        statsMap += (("collectThreshold", stats.parameters.collectThreshold.toString.replace('.', ',')))
      } else {
        println(stats)
      }
      g.shutdown
      statsMap
    }

    var result = List[Map[String, String]]()
    
    //run warm up
    for(_ <- 0 until 20) {
      runEvaluation()
    }
    
    //measure
    result = runEvaluation() :: result

    result
  }
}
