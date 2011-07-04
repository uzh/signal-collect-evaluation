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

package signalcollect.evaluation.jobsubmission

import org.apache.commons.codec.binary.Base64
import signalcollect.api.DefaultBuilder
import signalcollect.api.ComputeGraphBuilder
import signalcollect.evaluation.configuration.JobConfiguration
import signalcollect.evaluation.util.Serializer
import scala.util.Random
import signalcollect.evaluation.Evaluation
import scala.sys.process._

sealed trait ExecutionLocation
object LocalHost extends ExecutionLocation
case class Kraken(username: String = System.getProperty("user.name")) extends ExecutionLocation

abstract class OneClickEval {

  def createConfigurations: List[JobConfiguration]
  lazy val jobDescription: String = Random.nextInt.abs.toString
  lazy val executionLocation: ExecutionLocation = LocalHost

  lazy val mainClass = "signalcollect.evaluation.Evaluation"
  lazy val packagename = "evaluation-0.0.1-SNAPSHOT"
  lazy val jarSuffix = "-jar-with-dependencies.jar"
  lazy val fileSpearator = System.getProperty("file.separator")
  lazy val localhostJarname = packagename + jarSuffix
  lazy val krakenJarname = packagename + "-" + jobDescription + jarSuffix
  lazy val localJarpath = "." + fileSpearator + "target" + fileSpearator + localhostJarname

  def executeEvaluation {
    executionLocation match {
      case LocalHost => executeLocally
      case Kraken(username) => executeKraken(username)
    }
  }

  def executeLocally {
    val configurations = createConfigurations
    for (configuration <- configurations) {
      val eval = new Evaluation
      eval.execute(configuration)
    }
  }

  def executeKraken(krakenUsername: String) {
    /** PACKAGE EVAL CODE AS JAR */
    val commandPackage = "mvn -Dmaven.test.skip=true clean package"
    println(commandPackage)
    commandPackage !!

    /** COPY EVAL JAR TO KRAKEN */
    val commandCopy = "scp -v " + localJarpath + " " + krakenUsername + "@kraken.ifi.uzh.ch:" + krakenJarname
    println(commandCopy)
    commandCopy !!

    /** LOG INTO KRAKEN WITH SSH */
    val krakenShell = new SshShell(username = krakenUsername)

    /** IMPLEMENT THIS FUNCTION: CREATES ALL THE EVALUATION CONFIGURATIONS */
    val configurations = createConfigurations

    /** SUBMIT AN EVALUATION JOB FOR EACH CONFIGURATION */
    for (configuration <- configurations) {
      val serializedConfig = Serializer.write(configuration)
      val base64Config = Base64.encodeBase64String(serializedConfig).replace("\n","").replace("\r","")
      val script = getShellScript(configuration.jobId.toString, krakenJarname, mainClass, base64Config)
      val scriptBase64 = Base64.encodeBase64String(script.getBytes).replace("\n","").replace("\r","")
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
#PBS -l walltime=604800
#PBS -l cput=2400000
#PBS -l mem=20gb, vmem=20gb
#PBS -j oe
#PBS -m b
#PBS -m e
#PBS -m a
#PBS -V

jarname=""" + jarname + """
mainClass=""" + mainClass + """
serializedConfiguration=""" + serializedConfiguration + """
working_dir=`mktemp -d --tmpdir=/var/tmp`
vm_args="-Xmx35000m -Xms35000m"

# copy jar
cp ~/$jarname $working_dir/

# run test
cmd="java $vm_args -cp $working_dir/$jarname $mainClass $serializedConfiguration"
$cmd

# remove temporary directory
rm -rdf $working_dir
"""
    script
  }

}