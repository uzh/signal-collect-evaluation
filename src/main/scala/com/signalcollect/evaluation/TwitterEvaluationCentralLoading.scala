package com.signalcollect.evaluation

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.io.InputStream
import java.io.InputStreamReader
import com.signalcollect.Graph
import com.signalcollect.examples.PlaceholderEdge
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory
import com.signalcollect.configuration.ExecutionMode
import com.signalcollect.ExecutionInformation
import com.signalcollect.ExecutionConfiguration
import com.signalcollect.GraphEditor
import java.net.URL
import com.signalcollect.GraphBuilder
import akka.event.Logging
import com.signalcollect.Edge
import java.net.URLConnection
import java.io.FileOutputStream
import java.nio.channels.Channels
import java.io.FileInputStream
import java.util.zip.ZipInputStream
import scala.async.Async.async
import scala.concurrent.ExecutionContext.Implicits.global
import scala.sys.process._

object TwitterEvaluationCentralLoading extends EvaluationTemplate {
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
    val bulksize = 5000
    addStat("eagerIdle", eagerIdleDetection.toString)
    addStat("throttling", throttling.toString)
    addStat("heartbeatInterval", heartbeat.toString)
    addStat("bulksize", bulksize.toString)
    gb
      .withMessageBusFactory(new BulkAkkaMessageBusFactory(bulksize, false))
      .withLoggingLevel(Logging.DebugLevel)
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
    val reader = FileDownloader.getReader
    graph.awaitIdle
    var cnt = 0
    while (cnt < 1500000 && reader.hasNext) {
      if (cnt % 100000 == 0) log.info(s"loaded $cnt edges")
      if (cnt % 2000 == 0) Thread.sleep(1)
      addEdge(reader.next, graph)
      cnt += 1
    }
    graph
  }

  def addEdge(line: String, graph: Graph[Any, Any]) {
    if (!line.contains("#")) {
      val vertices = line.split("\\s+")
      graph.addEdge(vertices(1).toInt, new PlaceholderEdge(vertices(0).toInt))
    }
  }

}

object FileDownloader extends com.signalcollect.logging.Logging {

  def getBufferedReaderIterator {
    new BufferedReaderIterator(new BufferedReader(new InputStreamReader(new FileInputStream(new File("twitter_rv.net")))))
  }

  def downloadZipFile(from: String, to: String): ZipInputStream = {

    val url = new URL(from)
    val rbc = Channels.newChannel(url.openStream())
    val fos = new FileOutputStream(to)
    log.info("start download")
    progress(fos)
    fos.getChannel().transferFrom(rbc, 0, Long.MaxValue)
    log.info("download finished")
    fos.close()
    rbc.close()
    new ZipInputStream(new FileInputStream(to))
  }

  def progress(fos: FileOutputStream) {
    async {
      var cnt = 0
      while (cnt < 1500) {
        if (cnt % 10 == 0) log.info("ls -la".!!)
        fos.flush()
        Thread.sleep(2000)
        cnt += 1

      }
    }
  }

  def getReader: BufferedReaderIterator = {
    getBufferedReaderIterator(downloadZipFile("http://an.kaist.ac.kr/~haewoon/release/twitter_social_graph/twitter_rv.zip", "twitter_rv.zip"))
  }

  def getBufferedReaderIterator(zis: ZipInputStream): BufferedReaderIterator = {
    val entry = zis.getNextEntry
    new BufferedReaderIterator(new BufferedReader(new InputStreamReader(zis)))
  }
}