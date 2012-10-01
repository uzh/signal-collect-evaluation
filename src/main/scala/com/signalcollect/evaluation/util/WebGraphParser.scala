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
import java.util.zip.GZIPInputStream

/**
 * Loads the specified range of splits of the web graph.
 */
class WebGraphParser(inputFolder: String, externalLoggingFilePath: Option[String] = None, splitsToParse: Range) extends OptimizedGraphProvider {

  def populate(graph: Graph, combinedVertexBuilder: (Int, Array[Int]) => Vertex[_, _]) {
    for (workerId <- splitsToParse.par) {
      graph.loadGraph(Some(workerId), (new WebGraphParserHelper(inputFolder, externalLoggingFilePath)).parserForSplit(workerId, combinedVertexBuilder))
    }
    graph.awaitIdle
    val memoryUsed = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory().asInstanceOf[Double] / 1073741824
  }

  
}

/**
 * Prevents closure capture of the DefaultGraph class.
 */
case class WebGraphParserHelper(inputFolder: String, externalLoggingFilePath: Option[String] = None) {
  
  def parserForSplit(splitNumber: Int, combinedVertexBuilder: (Int, Array[Int]) => Vertex[_, _]): GraphEditor => Unit = {
    graphEditor => parseFile(graphEditor, "input_pt_" + splitNumber + ".txt.gz", combinedVertexBuilder)
  }
  
  def parseFile(graphEditor: GraphEditor, filename: String, combinedVertexBuilder: (Int, Array[Int]) => Vertex[_, _]) {
    //initialize input reader
    logStatus("started parsing " + filename)
    val fstream = new FileInputStream(inputFolder + System.getProperty("file.separator") + filename)
    val in = new DataInputStream(new GZIPInputStream(fstream))
    val input = new BufferedReader(new InputStreamReader(in))
    var line = input.readLine
    var verticesRead = 0
    while (line != null) {
      val vertexData = line.split(" ")
      val id = getInt(vertexData(0))
      val numberOfLinks = getInt(vertexData(1))
      val outlinks = new Array[Int](numberOfLinks)

      var i = 0
      while (i < numberOfLinks) {
        outlinks(i) = getInt(vertexData(i + 2))
        i += 1
      }
      val vertex = combinedVertexBuilder(id, outlinks)
      graphEditor.addVertex(vertex)

      verticesRead += 1

      if (verticesRead % 100000 == 0) {
        logStatus(filename + ": loaded " + verticesRead)
      }

      line = input.readLine
    }
    input.close
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
      logger.write(timeFormat.format(new Date) + " - " + msg + "\n")
      logger.close
    } else {
      println(msg)
    }
  }
}