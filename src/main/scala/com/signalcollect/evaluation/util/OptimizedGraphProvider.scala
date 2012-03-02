package com.signalcollect.evaluation.util

import com.signalcollect._
import scala.Serializable

trait OptimizedGraphProvider extends Serializable {
	def populateGraph(builder: GraphBuilder, combinedVertexBuilder: (Int, List[Int]) => Vertex): Graph
}