package com.signalcollect.evaluation

import com.signalcollect.evaluation.algorithms.MemoryMinimalPage

object LoaderTest extends App {
  val loader = CompressedSplitLoader("./", 0, (id, edgeIds) => {
    println(s"Read $id"); val v = new MemoryMinimalPage(id); v.setTargetIdArray(edgeIds); v
  })
  for (v <- loader) {
    print("---------")
  }
}
