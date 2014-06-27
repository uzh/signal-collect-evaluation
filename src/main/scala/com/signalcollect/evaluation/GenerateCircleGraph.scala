package com.signalcollect.evaluation

import com.google.gdata.data.Source
import java.io.FileWriter
import java.io.PrintWriter

object GenerateCircleGraph extends App {
  val length = args(0).toInt
  val writer = new FileWriter(s"./circle-graph-$length")
  val printer = new PrintWriter(writer)
  for (i <- 1 until length) {
    printer.println(s"$i ${i + 1}")
  }
  printer.println(s"$length 1")
  printer.close
}
