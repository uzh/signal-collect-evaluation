package com.signalcollect.visualization

import com.signalcollect.configuration.DefaultComputeGraphBuilder
import com.signalcollect.examples.Page
import com.signalcollect.examples.Link

object VisualizerTest extends App {

  val cg = DefaultComputeGraphBuilder.build
  cg.addVertex(new Page(1))
  cg.addVertex(new Page(2))
  cg.addVertex(new Page(3))
  cg.addEdge(new Link(1, 2))
  cg.addEdge(new Link(2, 1))
  cg.addEdge(new Link(2, 3))
  cg.addEdge(new Link(3, 2))
  val visualizer = new GraphVisualizer(new ComputeGraphInspector(cg)).setVisible(true)
  val stats = cg.execute
  println(stats)
  cg.foreachVertex(println(_))
  cg.shutdown

}