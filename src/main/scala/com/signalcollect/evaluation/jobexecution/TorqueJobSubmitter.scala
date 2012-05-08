/*
 *  @author Daniel Strebel
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

import scala.sys.process._
import ch.ethz.ssh2.Connection
import java.io.File
import ch.ethz.ssh2.StreamGobbler
import com.signalcollect.evaluation.jobsubmission.IoUtil
import org.apache.commons.codec.binary.Base64

/**
 * Determines the priority in torque's scheduling queue
 */
object TorquePriority {
  val superfast = "#PBS -l walltime=00:59:59,mem=50gb"
  val fast = "#PBS -l walltime=11:59:59,mem=50gb"
  val slow = "#PBS -l walltime=200:59:59,mem=50gb"
}

class TorqueJobSubmitter(
  val username: String,
  val mailAddress: String,
  val hostname: String,
  val port: Int = 22,
  val privateKeyFilePath: String = System.getProperty("user.home") + System.getProperty("file.separator") + ".ssh" + System.getProperty("file.separator") + "id_rsa") {

  def copyFileToCluster(localPath: String, targetPath: String = "") {
    val commandCopy = "scp -v " + localPath + " " + username + "@" + hostname + ":" + targetPath
    println(commandCopy)
    println(commandCopy !!)
  }

  def executeCommandOnClusterManager(command: String): String = {
    val connection = connectToHost
    val session = connection.openSession
    session.execCommand(command)
    val result = IoUtil.streamToString(new StreamGobbler(session.getStdout)) + "\n" + IoUtil.streamToString(new StreamGobbler(session.getStderr))
    session.close
    connection.close
    result
  }
  
  def runOnClusterNode(jobId: String, jarname: String, mainClass: String, priority: String = TorquePriority.superfast, jvmParameters: String): String = {
    val script = getShellScript(jobId, jarname, mainClass, priority, jvmParameters)
    val scriptBase64 = Base64.encodeBase64String(script.getBytes).replace("\n", "").replace("\r", "")
    val qsubCommand = """echo """ + scriptBase64 + """ | base64 -d | qsub"""
    executeCommandOnClusterManager(qsubCommand)
  }

  protected def connectToHost: Connection = {
    val connection = new Connection(hostname, port)
    connection.connect
    connection.authenticateWithPublicKey(username, new File(privateKeyFilePath), null)
    connection
  }
  
    def getShellScript(jobId: String, jarname: String, mainClass: String, priority: String, jvmParameters: String): String = {
    val script = """
#!/bin/bash
#PBS -N """ + jobId + """
#PBS -l nodes=1:ppn=23
""" + priority + """
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
vm_args="""" + jvmParameters + """ -Xmx65000m -Xms65000m -d64"

# copy jar
cp ~/$jarname $workingDir/

# run test
cmd="java $vm_args -cp $workingDir/$jarname $mainClass """ + jobId + """"
$cmd
"""
    script
  }
}