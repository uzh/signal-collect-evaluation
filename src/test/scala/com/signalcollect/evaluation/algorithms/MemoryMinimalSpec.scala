package com.signalcollect.evaluation.algorithms

import com.signalcollect._
import com.signalcollect.configuration._
import com.signalcollect.interfaces._
import com.signalcollect.graphproviders._
import collection.JavaConversions._
import org.specs2.mutable._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import com.signalcollect.evaluation.algorithms._

/**
 * Hint: For information on how to run specs see the specs v.1 website
 * http://code.google.com/p/specs/wiki/RunningSpecs
 */
@RunWith(classOf[JUnitRunner])
class MemoryMinimalPageSpec extends SpecificationWithJUnit with Serializable {
  "PageRank algorithm" should {
    "deliver correct results on a 5-cycle graph" in {
      println("PageRank algorithm on a 5-cycle graph")
      def pageRankFiveCycleVerifier(v: Vertex): Boolean = {
        val state = v.state.asInstanceOf[Float]
        val expectedState = 1.0
        val correct = (state - expectedState).abs < 0.00001f
        if (!correct) {
          System.out.println("Problematic vertex:  id=" + v.id + ", expected state=" + expectedState + " actual state=" + state)
        }
        correct
      }
      
      val graph = GraphBuilder.build
      for (i <- 0 until 5) {
        val v = new MemoryMinimalPage(i)
        v.setTargetIdArray(Array((i+1)%5))
        graph.addVertex(v)
      }
      
      graph.execute(ExecutionConfiguration.withCollectThreshold(0).withSignalThreshold(0))
      var allcorrect = graph.aggregate(new AggregationOperation[Boolean] {
          val neutralElement = true
          def aggregate(a: Boolean, b: Boolean): Boolean = a && b
          def extract(v: Vertex): Boolean = pageRankFiveCycleVerifier(v)
        })
      allcorrect
    }
  }
}