/*
 *  @author Philip Stutz
 *  @author Lorenz Fischer
 *  
 *  Copyright 2010 University of Zurich
 *      
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 */

package com.signalcollect.evaluation.jobsubmission

import org.apache.commons.codec.binary.Base64
import com.signalcollect.configuration._
import com.signalcollect.evaluation.configuration.Job
import scala.util.Random
import scala.sys.process._
import com.signalcollect.evaluation.jobexecution.JobExecutor
import java.io.File
import com.signalcollect.implementations.serialization.DefaultSerializer

sealed trait ExecutionLocation
object LocalHost extends ExecutionLocation
case class Kraken(username: String = System.getProperty("user.name")) extends ExecutionLocation

class JobSubmitter(
  executionLocation: ExecutionLocation = Kraken(),
  val recompileCore: Boolean = true,
  val jarDescription: String = Random.nextInt.abs.toString,
  val pathToSignalcollectCorePom: String = new File("../core/pom.xml").getCanonicalPath, // maven -file CLI parameter can't relative paths
  val mainClass: String = "com.signalcollect.evaluation.jobexecution.JobExecutor",
  val packagename: String = "evaluation-0.0.1-SNAPSHOT") {

  lazy val jarSuffix = "-jar-with-dependencies.jar"
  lazy val fileSpearator = System.getProperty("file.separator")
  lazy val localhostJarname = packagename + jarSuffix
  lazy val krakenJarname = packagename + "-" + jarDescription + jarSuffix
  lazy val localJarpath = "." + fileSpearator + "target" + fileSpearator + localhostJarname

  def submitJobs(jobs: List[Job]) {
    executionLocation match {
      case LocalHost => executeLocally(jobs)
      case Kraken(username) => executeKraken(username, jobs)
    }
  }

  def executeLocally(jobs: List[Job]) {
    val executor = new JobExecutor
    for (job <- jobs) {
      executor.run(job)
    }
  }

  def executeKraken(krakenUsername: String, jobs: List[Job]) {
    if (recompileCore) {
      val commandInstallCore = "mvn -file " + pathToSignalcollectCorePom + " -Dmaven.test.skip=true clean install"
      println(commandInstallCore)
      println(commandInstallCore !!)
    }

    /** PACKAGE EVAL CODE AS JAR */
    val commandPackage = "mvn -Dmaven.test.skip=true clean package"
    println(commandPackage)
    println(commandPackage !!)

    /** COPY EVAL JAR TO KRAKEN */
    val commandCopy = "scp -v " + localJarpath + " " + krakenUsername + "@kraken.ifi.uzh.ch:" + krakenJarname
    println(commandCopy)
    println(commandCopy !!)

    /** LOG INTO KRAKEN WITH SSH */
    val krakenShell = new SshShell(username = krakenUsername)

    /** SUBMIT AN EVALUATION JOB FOR EACH CONFIGURATION */
    for (job <- jobs) {
      val serializedConfig = DefaultSerializer.write(job)
      val base64Config = Base64.encodeBase64String(serializedConfig).replace("\n", "").replace("\r", "")
      val script = getShellScript(job.jobId.toString, krakenJarname, mainClass, base64Config)
      val scriptBase64 = Base64.encodeBase64String(script.getBytes).replace("\n", "").replace("\r", "")
      val qsubCommand = """echo """ + scriptBase64 + """ | base64 -d | qsub"""
      println(krakenShell.execute(qsubCommand))
    }
    /** LOG OUT OF KRAKEN */
    krakenShell.exit
  }

  def getShellScript(jobId: String, jarname: String, mainClass: String, serializedConfiguration: String): String = {
    val script = """
#!/bin/bash
#PBS -N """ + jobId + """
#PBS -l nodes=1:ppn=23
#PBS -l walltime=36000,cput=2400000,mem=20gb
#PBS -j oe
#PBS -m b
#PBS -m e
#PBS -m a
#PBS -V

jarname=""" + jarname + """
mainClass=""" + mainClass + """
serializedConfiguration=""" + serializedConfiguration + """
workingDir=/home/torque/tmp/${USER}.${PBS_JOBID}
vm_args="-Xmx35000m -Xms35000m -d64"

# copy jar
cp ~/$jarname $workingDir/

# run test
cmd="java $vm_args -cp $workingDir/$jarname $mainClass $serializedConfiguration"
$cmd
"""
    script
  }

}