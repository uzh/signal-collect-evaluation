package com.signalcollect.evaluation.util

import com.signalcollect._
import scala.Serializable

trait OptimizedGraphProvider extends Serializable {
	def populate(graph: Graph, combinedVertexBuilder: (Int, List[Int]) => Vertex[_, _])
}