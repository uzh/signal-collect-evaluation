package com.signalcollect.evaluation.util

import com.signalcollect._
import scala.Serializable

trait OptimizedGraphProvider[Id, Signal] extends Serializable {
	def populate(graphEditor: GraphEditor[Id, Signal], combinedVertexBuilder: (Int, Array[Int]) => Vertex[Int, _])
}