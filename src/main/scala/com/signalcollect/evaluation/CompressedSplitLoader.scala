package com.signalcollect.evaluation

import com.signalcollect.GraphEditor
import com.signalcollect.Vertex
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.io.EOFException
import com.signalcollect.evaluation.algorithms.MemoryMinimalPage
import java.io.BufferedInputStream
import java.io.DataInputStream

case class CompressedSplitLoader(
  inputFolder: String, splitId: Int, combinedVertexBuilder: (Int, Array[Int]) => Vertex[Int, _])
  extends Iterator[GraphEditor[Int, Float] => Unit] {

  def splitPath = inputFolder + System.getProperty("file.separator") + "input_pt_" + splitId + ".txt.gz"
  var gzipIn: GZIPInputStream = _
  var bufferedInput: BufferedInputStream = _ //new BufferedInputStream(gzipIn)
  var in: DataInputStream = _ //new DataInputStream(bufferedInput)

  var isInitialized = false

  protected def readNextVertex: Vertex[Int, _] = {
    try {
      val id = in.readInt
      val numberOfLinks = in.readInt
      val outlinks = new Array[Int](numberOfLinks)
      var i = 0
      while (i < numberOfLinks) {
        outlinks(i) = in.readInt
        i += 1
      }
      combinedVertexBuilder(id, outlinks)
    } catch {
      case done: EOFException =>
        println(s"Split #$splitId was fully read.")
        in.close
        bufferedInput.close
        gzipIn.close
        null.asInstanceOf[Vertex[Int, _]]
      case t: Throwable =>
        println(s"Error during reading of split #$splitId:\n" + t.getStackTrace.mkString("\n"))
        null.asInstanceOf[Vertex[Int, _]]
    }
  }

  var nextVertex: Vertex[Int, _] = null

  def initialize {
    gzipIn = new GZIPInputStream(new FileInputStream(splitPath))
    bufferedInput = new BufferedInputStream(gzipIn)
    in = new DataInputStream(bufferedInput)
    nextVertex = readNextVertex
    isInitialized = true
  }

  def hasNext = {
    if (!isInitialized) {
      initialize
    }
    nextVertex != null
  }

  def next: GraphEditor[Int, Float] => Unit = {
    if (!isInitialized) {
      initialize
    }
    if (nextVertex == null) {
      throw new Exception("next was called when hasNext is false.")
    }
    val v = nextVertex // This is actually important, so the closure doesn't capture the mutable var.
    val loader: GraphEditor[Int, Float] => Unit = { ge =>
      ge.addVertex(v)
    }
    nextVertex = readNextVertex
    loader
  }

}
