package com.signalcollect.evaluation

import scala.reflect.runtime.universe
import java.io.File
import java.lang.management.GarbageCollectorMXBean
import java.lang.management.ManagementFactory
import java.util.Date
import scala.Option.option2Iterable
import scala.io.Source
import com.signalcollect.GraphBuilder
import com.signalcollect.deployment.TorqueDeployableAlgorithm
import akka.actor.ActorRef
import collection.JavaConversions._

object EvalHelpers {

  def getObject[T](o: String): T = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(o)
    val obj = runtimeMirror.reflectModule(module).instance
    obj.asInstanceOf[T]
  }

  def getRevision: String = {
    try {
      val gitLogPath = ".git/logs/HEAD"
      val gitLog = new File(gitLogPath)
      val lines = Source.fromFile(gitLogPath).getLines
      val lastLine = lines.toList.last
      val revision = lastLine.split(" ")(1)
      revision
    } catch {
      case t: Throwable => "Unknown revision."
    }
  }

  def bytesToGigabytes(bytes: Long): Double = ((bytes / 1073741824.0) * 10.0).round / 10.0

  def cleanGarbage {
    for (i <- 1 to 10) {
      System.runFinalization
      System.gc
      Thread.sleep(10000)
    }
    Thread.sleep(120000)
  }

  /**
   * Returns the time in milliseconds it takes to execute the code in 'codeBlock'.
   */
  def measureTime(codeBlock: => Unit): Long = {
    val startTime = System.currentTimeMillis
    codeBlock
    val finishTime = System.currentTimeMillis
    finishTime - startTime
  }

  def roundToMillisecondFraction(nanoseconds: Long): Double = {
    ((nanoseconds / 100000.0).round) / 10.0
  }

  def getGcCollectionTime(gcs: List[GarbageCollectorMXBean]): Long = {
    gcs.map(_.getCollectionTime).sum
  }

  def lastGcId(gcs: List[GarbageCollectorMXBean]): Long = {
    val sunGcs = gcs map (_.asInstanceOf[com.sun.management.GarbageCollectorMXBean])
    val gcIds = sunGcs.
      map(_.getLastGcInfo).
      flatMap(info => if (info != null) Some(info.getId) else None)
    if (gcIds.isEmpty) 0 else gcIds.max
  }

  def freedDuringLastGc(gcs: List[GarbageCollectorMXBean]): Long = {
    val sunGcs = gcs map (_.asInstanceOf[com.sun.management.GarbageCollectorMXBean])
    val usedBeforeLastGc = sunGcs.
      map(_.getLastGcInfo).
      map(_.getMemoryUsageBeforeGc).
      flatMap(_.values).
      map(_.getCommitted).
      sum
    val usedAfterLastGc = sunGcs.
      map(_.getLastGcInfo).
      map(_.getMemoryUsageAfterGc).
      flatMap(_.values).
      map(_.getCommitted).
      sum
    val freedDuringLastGc = usedBeforeLastGc - usedAfterLastGc
    freedDuringLastGc
  }

  def getGcCollectionCount(gcs: List[GarbageCollectorMXBean]): Long = {
    gcs.map(_.getCollectionCount).sum
  }

}
