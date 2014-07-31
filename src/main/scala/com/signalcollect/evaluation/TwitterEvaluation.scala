package com.signalcollect.evaluation

import java.io.BufferedReader
import java.io.FileOutputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URL
import java.net.URLConnection
import java.nio.channels.Channels
import com.signalcollect.Edge
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.ExecutionInformation
import com.signalcollect.Graph
import com.signalcollect.GraphBuilder
import com.signalcollect.GraphEditor
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.examples.PlaceholderEdge
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import akka.event.Logging
import java.io.FileInputStream

object TwitterEvaluation extends EvaluationTemplate {
  override def deploy: Boolean = true

  override def beforeStart {
    super.beforeStart
    addStat("dataset", "Twitter")
  }

  override def execute(g: Graph[Any, Any]): (ExecutionInformation, Graph[Any, Any]) = {
    log.info("starting execution")
    val executionMode = ExecutionMode.OptimizedAsynchronous
    addStat("executionMode", executionMode.toString)
    val stats = g.execute(ExecutionConfiguration.withExecutionMode(executionMode))
    log.info("execution finished now awaiting idle")
    (stats, g)
  }

  override def configureGraphBuilder(gb: GraphBuilder[Any, Any]): GraphBuilder[Any, Any] = {
    val eagerIdleDetection = false
    val throttling = true
    val heartbeat = 100
    val bulksize = 80000
    addStat("eagerIdle", eagerIdleDetection.toString)
    addStat("throttling", throttling.toString)
    addStat("heartbeatInterval", heartbeat.toString)
    addStat("bulksize", bulksize.toString)
    gb
      .withMessageBusFactory(new BulkAkkaMessageBusFactory(bulksize, false))
      .withLoggingLevel(Logging.InfoLevel)
      .withEagerIdleDetection(eagerIdleDetection)
      .withThrottlingEnabled(throttling)
      .withHeartbeatInterval(heartbeat)
  }

  override def loadGraph(graph: Graph[Any, Any]): Graph[Any, Any] = {
    graph.setEdgeAddedToNonExistentVertexHandler {

      Handlers.nonExistingVertex

    }
    graph.setUndeliverableSignalHandler {
      Handlers.undeliverableSignal
    }
    graph.awaitIdle
    var a = 0
    for (a <- 0 to 63) {
      log.info(s"begin loading of split $a")
      val loader = TwitterLoader(a).asInstanceOf[Iterator[GraphEditor[Any, Any] => Unit]]
      graph.loadGraph(loader, Some(a*2))
    }
    graph
  }

}

case class TwitterLoader(splitId: Int) extends Iterator[GraphEditor[Int, Double] => Unit] with com.signalcollect.logging.Logging {
  val split = splitId.toString
  lazy val filename = if (split.length == 1) "x0" + split else "x" + split
  lazy val url = new URL(s"https://s3-eu-west-1.amazonaws.com/signalcollect/user/hadoop/$filename")
  lazy val in = {
    log.info(s"start trying download $filename")
    var downloaded = false
    while (!downloaded) {
      val rbc = Channels.newChannel(url.openStream())
      val fos = new FileOutputStream(filename)
      try {
        log.info("start download")
        fos.getChannel().transferFrom(rbc, 0, Long.MaxValue)
        log.info("download finished")
        downloaded = true
      } catch {
        case e: Throwable => {
          log.error("failed to download retry in 1s")
          Thread.sleep(1000)
        }
      } finally {
        log.info("closing download connection")
        fos.close()
        rbc.close()
      }
    }
    new BufferedReader(
      new InputStreamReader(new FileInputStream(filename)))
  }
  var line = ""
  var cnt = 0
  var done = false

  def addEdge(vertices: (Int, Int))(graphEditor: GraphEditor[Int, Double]) {
    graphEditor.addEdge(vertices._2, new PlaceholderEdge[Int](vertices._1).asInstanceOf[Edge[Int]],false)
  }

  def hasNext = {
    val next = line != null
    if (!next && !done) {
      log.info(s"done loading $filename")
      done = true
      in.close()
    }
    next
  }

  def next: GraphEditor[Int, Double] => Unit = {
    if (line == "") {
      line = in.readLine()
      log.info(s"read split $filename")
    }
    val ids = line.split("\\s+")

    val edge = (ids(0).toInt, ids(1).toInt)
    if (cnt % 2000000 == 0) log.info(s"loaded $cnt edges")
    cnt += 1
    line = in.readLine()
    addEdge(edge) _
  }
}