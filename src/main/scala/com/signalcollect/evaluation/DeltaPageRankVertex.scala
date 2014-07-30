package com.signalcollect.evaluation

import com.signalcollect.DataFlowVertex

class DeltaPageRankVertex(override val id: Int, initialState: Double)
  extends DataFlowVertex(id, initialState) {

  type Signal = Double

  def collect(delta: Double) = {
    state + delta
  }

  override def scoreSignal: Double = {
    if (lastSignalState.isDefined) {
      state - lastSignalState.get
    } else {
      1
    }
  }

}