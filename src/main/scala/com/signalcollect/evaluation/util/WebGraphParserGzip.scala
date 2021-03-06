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
package com.signalcollect.evaluation.util

import com.signalcollect._
import java.io._
import scala.collection.mutable.ArrayBuffer
import java.text.SimpleDateFormat
import java.util.Date
import scala.sys.process._
import java.util.zip.ZipFile
import java.util.zip.ZipEntry
import java.util.zip.GZIPInputStream
import org.apache.commons.io.FileUtils

/**
 * Loads the specified range of splits of the web graph.
 */
class WebGraphParserGzip[VertexState, Signal](inputFolder: String, externalLoggingFilePath: Option[String] = None, splitsToParse: Int, numberOfWorkers: Int, graphName: String = "WebGraphParser") {

  def populate(graph: Graph[Int, Signal], combinedVertexBuilder: (Int, Array[Int]) => Vertex[Int, _, Int, Signal]) {
    println("started loading " + splitsToParse + " splits by WebGraphParserGzip")
    for (splitId <- 100 until 140) {
      //    for (splitId <- 0 until splitsToParse) {
      val workerId = splitId % numberOfWorkers
      //println("sending load command for " + splitId + " to worker " + workerId)
      graph.modifyGraph((new WebGraphParserHelperGzip(inputFolder, externalLoggingFilePath)).parserForSplit(splitId, combinedVertexBuilder), Some(workerId))
      //println("load command for " + splitId + " was sent to worker " + workerId)
      //      if (splitId % numberOfWorkers == numberOfWorkers - 1) {
      //        println("awaiting idle ...")
      //        graph.awaitIdle
      //        println("done.")
      //      }
    }
    //    }
    println("Loading in progress, awaiting idle ...")
    graph.awaitIdle
    println("Graph has been loaded.")
    val usedMemory = (Runtime.getRuntime.totalMemory - Runtime.getRuntime.freeMemory) / 131072
    println("Used memory in MB " + usedMemory)
  }

  def logStatus(msg: String) {
    if (externalLoggingFilePath.isDefined) {
      val timeFormat = new SimpleDateFormat("HH:mm:ss")
      val logFileWiter = new FileWriter(externalLoggingFilePath.get, true)
      val logger = new BufferedWriter(logFileWiter)
      logger.write(timeFormat.format(new Date) + " - " + msg + "\n")
      logger.close
    } else {
      println(msg)
    }
  }

  override def toString = graphName

}

/**
 * Prevents closure capture of the DefaultGraph class.
 */
case class WebGraphParserHelperGzip[Signal](inputFolder: String, externalLoggingFilePath: Option[String] = None, testMode: Boolean = false) {

  var startTimeLoading: Date = null

  def parserForSplit(splitNumber: Int, combinedVertexBuilder: (Int, Array[Int]) => Vertex[Int, _, Int, Signal]): GraphEditor[Int, Signal] => Unit = {
    graphEditor => parseFile(graphEditor, "input_pt_" + splitNumber + ".txt.gz", combinedVertexBuilder)
  }

  def parseFile(graphEditor: GraphEditor[Int, Signal], filename: String, combinedVertexBuilder: (Int, Array[Int]) => Vertex[Int, _, Int, Signal]) {
    //initialize input reader
    startTimeLoading = new Date()
    println("started parsing " + filename)
    logStatus("started parsing " + filename)

    val gzipIn = new GZIPInputStream(new FileInputStream(inputFolder + System.getProperty("file.separator") + filename))
    val bufferedInput = new BufferedInputStream(gzipIn)
    val in = new DataInputStream(bufferedInput)

    var verticesRead = 0
    var edgesRead = 0

    try {
      while (true) {
        val id = in.readInt
        val numberOfLinks = in.readInt
        val outlinks = new Array[Int](numberOfLinks)
        var i = 0
        while (i < numberOfLinks) {
          outlinks(i) = in.readInt
          i += 1
        }
        if (!testMode) {
          val vertex = combinedVertexBuilder(id, outlinks)
          graphEditor.addVertex(vertex, true)
        }
        verticesRead += 1
        edgesRead += numberOfLinks

        if (verticesRead % 100000 == 0) {
          logStatus(filename + ": loaded " + verticesRead)
        }
      }

    } catch {
      case e: EOFException => {} //Reached end of file.
      case exception: Exception => exception.printStackTrace()
    }
    in.close
    logStatus("done parsing " + filename)
  }

  protected def getInt(s: String): Int = {
    Integer.valueOf(s)
  }

  def logStatus(msg: String) {
    if (externalLoggingFilePath.isDefined) {
      val timeFormat = new SimpleDateFormat("HH:mm:ss")
      val logFileWiter = new FileWriter(externalLoggingFilePath.get, true)
      val logger = new BufferedWriter(logFileWiter)
      logger.write(timeFormat.format((new Date).getTime() - startTimeLoading.getTime()) + " - " + msg + "\n")
      logger.close
    } else {
      println(msg)
    }
  }
}