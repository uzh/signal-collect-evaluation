package com.signalcollect.evaluation.util

import com.signalcollect._
import scala.Serializable

trait OptimizedGraphProvider extends Serializable {
	def populate(graphEditor: GraphEditor, combinedVertexBuilder: (Int, Array[Int]) => Vertex[_, _])
}