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
import com.signalcollect.nodeprovisioning.torque.LocalHost
import com.signalcollect.nodeprovisioning.local.LocalNodeProvisioner
import com.signalcollect.nodeprovisioning.Node
import com.signalcollect.factory.messagebus.BulkAkkaMessageBusFactory

/**
 * Hint: For information on how to run specs see the specs v.1 website
 * http://code.google.com/p/specs/wiki/RunningSpecs
 */
@RunWith(classOf[JUnitRunner])
class CompactIntSetSpec extends SpecificationWithJUnit with Serializable {

  "Compact Int Set" should {
    "correctly encode and decode an empty array" in {
      var count = 0
      val set = CompactIntSet.create(Array())
      CompactIntSet.foreach(set, x => count += 1)
      count must_== 0
    }
    
    "encode and decode an array with one value" in {
      var count = 0
      val set = CompactIntSet.create(Array(0))
      CompactIntSet.foreach(set, i => count += 1)
      count must_== 1
    }

    "correctly encode and decode an array with only value 0" in {
      var number = -1
      val set = CompactIntSet.create(Array(0))
      CompactIntSet.foreach(set, i => number = i)
      number must_== 0
    }
    
    "correctly encode and decode an array with only value -1" in {
      var number = 0
      val set = CompactIntSet.create(Array(-1))
      CompactIntSet.foreach(set, i => number = i)
      number must_== -1
    }
    
    "correctly encode and decode an array with only value Int.MaxValue" in {
      var number = 0
      val set = CompactIntSet.create(Array(Int.MaxValue))
      CompactIntSet.foreach(set, i => number = i)
      number must_== Int.MaxValue
    }
    
    "correctly encode and decode an array with multiple values" in {
      var numbers = List[Int]()
      val set = CompactIntSet.create(Array(0, 1, 500, Int.MaxValue))
      CompactIntSet.foreach(set, i => numbers = i :: numbers)
      numbers.reverse must_== List(0, 1, 500, Int.MaxValue)
    }
    
    
  }
}