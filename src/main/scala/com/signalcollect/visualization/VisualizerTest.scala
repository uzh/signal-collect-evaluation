package com.signalcollect.visualization

import com.signalcollect.configuration.DefaultComputeGraphBuilder
import com.signalcollect.examples._

object VisualizerTest extends App {

  val cg = DefaultComputeGraphBuilder.build
//  cg.addVertex(new Page(1))
//  cg.addVertex(new Page(2))
//  cg.addVertex(new Page(3))
//  cg.addVertex(new Page(4))
//  cg.addVertex(new Page(5))
//  cg.addEdge(new Link(1, 2))
//  cg.addEdge(new Link(2, 3))
//  cg.addEdge(new Link(3, 4))
//  cg.addEdge(new Link(4, 5))
//  cg.addEdge(new Link(5, 1))
  
  cg.addVertex(new Location(1, Some(0)))
  cg.addVertex(new Location(2))
  cg.addVertex(new Location(3))
  cg.addVertex(new Location(4))
  cg.addVertex(new Location(5))
  cg.addVertex(new Location(6))
  cg.addEdge(new Path(1, 2))
  cg.addEdge(new Path(2, 3))
  cg.addEdge(new Path(3, 4))
  cg.addEdge(new Path(1, 5))
  cg.addEdge(new Path(4, 6))
  cg.addEdge(new Path(5, 6))
  
  val visualizer = new GraphVisualizer(new ComputeGraphInspector(cg)).setVisible(true)
}