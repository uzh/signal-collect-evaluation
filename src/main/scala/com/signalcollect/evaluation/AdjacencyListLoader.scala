package com.signalcollect.evaluation

import com.signalcollect.GraphEditor
import com.signalcollect.Vertex
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.io.EOFException
import com.signalcollect.evaluation.algorithms.MemoryMinimalPage
import java.io.BufferedInputStream
import java.io.DataInputStream
import com.signalcollect.util.FileReader

case class AdjacencyListLoader[SignalType](
  filePath: String, combinedVertexBuilder: (Int, Array[Int]) => Vertex[Int, _, Int, SignalType])
  extends Iterator[GraphEditor[Int, SignalType] => Unit] {

  var intIterator: Iterator[Int] = _

  var isInitialized = false

  protected def readNextVertex: Vertex[Int, _, Int, SignalType] = {
    val id = intIterator.next
    val numberOfLinks = intIterator.next
    val outlinks = new Array[Int](numberOfLinks)
    var i = 0
    while (i < numberOfLinks) {
      outlinks(i) = intIterator.next
      i += 1
    }
    combinedVertexBuilder(id, outlinks)
  }

  var nextVertex: Vertex[Int, _, Int, SignalType] = null

  def initialize {
    intIterator = FileReader.intIterator(filePath)
    isInitialized = true
    nextVertex = readNextVertex
  }

  def hasNext = {
    if (!isInitialized) {
      initialize
    }
    nextVertex != null
  }

  def next: GraphEditor[Int, SignalType] => Unit = {
    if (!isInitialized) {
      initialize
    }
    if (nextVertex == null) {
      throw new Exception("next was called when hasNext is false.")
    }
    val v = nextVertex // This is actually important, so the closure doesn't capture the mutable var.
    val loader: GraphEditor[Int, SignalType] => Unit = { ge =>
      ge.addVertex(v)
    }
    nextVertex = readNextVertex
    loader
  }

}
