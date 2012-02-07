/*
 *  @author Philip Stutz
 *  
 *  Copyright 2012 University of Zurich
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
package com.signalcollect.evaluation.jobexecution

import scala.util.Random
import java.io.File
import com.signalcollect.evaluation.configuration.Job
import scala.sys.process._
import org.apache.commons.codec.binary.Base64
import com.signalcollect.evaluation.jobsubmission.SshShell
import com.signalcollect.implementations.serialization.DefaultSerializer
import java.io.FileOutputStream

object KrakenHost

class KrakenHost(val krakenUsername: String = System.getProperty("user.name"),
  val mailAddress: String = "",
  val jvmParameters: String = "",
  val recompileCore: Boolean = true,
  val jarDescription: String = Random.nextInt.abs.toString,
  val pathToSignalcollectCorePom: String = new File("../core/pom.xml").getCanonicalPath, // maven -file CLI parameter can't relative paths
  val mainClass: String = "com.signalcollect.evaluation.jobexecution.JobExecutor",
  val packagename: String = "signal-collect-evaluation-2.0.0-SNAPSHOT") extends ExecutionHost {

  lazy val jarSuffix = "-jar-with-dependencies.jar"
  lazy val fileSpearator = System.getProperty("file.separator")
  lazy val localhostJarname = packagename + jarSuffix
  lazy val krakenJarname = packagename + "-" + jarDescription + jarSuffix
  lazy val localJarpath = "." + fileSpearator + "target" + fileSpearator + localhostJarname

  def executeJobs(jobs: List[Job]) = {
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
      val config = DefaultSerializer.write((job, resultHandlers))
      val out = new FileOutputStream(job.jobId + ".config")
      out.write(config)
      out.close
      val copyConfig = "scp -v " + job.jobId + ".config" + " " + krakenUsername + "@kraken.ifi.uzh.ch:"
      copyConfig !!
      val script = getShellScript(job.jobId.toString, krakenJarname, mainClass)
      val scriptBase64 = Base64.encodeBase64String(script.getBytes).replace("\n", "").replace("\r", "")
      val qsubCommand = """echo """ + scriptBase64 + """ | base64 -d | qsub"""
      println(krakenShell.execute(qsubCommand))
    }
    /** LOG OUT OF KRAKEN */
    krakenShell.exit
  }

  def getShellScript(jobId: String, jarname: String, mainClass: String): String = {
    val script = """
#!/bin/bash
#PBS -N """ + jobId + """
#PBS -l nodes=1:ppn=23
#PBS -l walltime=00:10:59,cput=24000,mem=50gb
#PBS -j oe
#PBS -m b
#PBS -m e
#PBS -m a
#PBS -V
#PBS -o out/""" + jobId + """.out
#PBS -e err/""" + jobId + """.err
""" + { if (mailAddress != null && mailAddress.length > 0) "#PBS -m a -M " + mailAddress else "" } + """

jarname=""" + jarname + """
mainClass=""" + mainClass + """
workingDir=/home/torque/tmp/${USER}.${PBS_JOBID}
vm_args="""" + jvmParameters + """ -Xmx35000m -Xms35000m -d64"

# copy jar
cp ~/$jarname $workingDir/

# run test
cmd="java $vm_args -cp $workingDir/$jarname $mainClass """ + jobId + """"
$cmd
"""
    script
  }
}