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
import signalcollect.api.DefaultSynchronousBuilder
import signalcollect.api.ComputeGraphBuilder
import signalcollect.evaluation.configuration.Configuration
import signalcollect.evaluation.util.Serializer

abstract class OneClickEval {

  def createConfigurations: List[Configuration]

  val mainClass = "signalcollect.evaluation.Evaluation"
  val packagename = "evaluation-0.0.1-SNAPSHOT"
  val jarSuffix = "-jar-with-dependencies.jar"
  val fileSpearator = System.getProperty("file.separator")
  val jarname = packagename + jarSuffix
  val localJarpath = "." + fileSpearator + "target" + fileSpearator + jarname

  def executeEvaluation {
    /** PACKAGE EVAL CODE AS JAR */
    val commandPackage = "mvn -Dmaven.test.skip=true clean package"
    println(commandPackage)
    var execution = Runtime.getRuntime.exec(commandPackage)
    IoUtil.printStream(execution.getInputStream)

    /** COPY EVAL JAR TO KRAKEN */

    val commandCopy = "scp -v " + localJarpath + " kraken.ifi.uzh.ch:"
    println(commandCopy)
    execution = Runtime.getRuntime.exec(commandCopy)
    IoUtil.printStream(execution.getInputStream)

    /** LOG INTO KRAKEN WITH SSH */
    val kraken = new SshShell

    /** IMPLEMENT THIS FUNCTION: CREATES ALL THE EVALUATION CONFIGURATIONS */
    val configurations = createConfigurations
    
    /** SUBMIT AN EVALUATION JOB FOR EACH CONFIGURATION */
    for (configuration <- configurations) {
      val jobJarname = packagename + "-" + configuration.jobId + jarSuffix
      val copyCommand = "cp " + jarname + " " + jobJarname
      println(copyCommand)
      println(kraken.execute(copyCommand))
      val serializedConfig = Serializer.write(configuration)
      val base64Config = Base64.encodeBase64String(serializedConfig)
      val script = getShellScript(jobJarname, mainClass, base64Config)
      val scriptBase64 = Base64.encodeBase64String(script.getBytes)
      println("ENCODED STRING: " + scriptBase64)
      val qsubCommand = """echo """" + scriptBase64 + """" | base64 -d | qsub"""
      println(qsubCommand)
      println(kraken.execute(qsubCommand))
    }

    /** CLEAN UP ON KRAKEN */
    val deleteJarCommand = "rm " + jarname
    println(deleteJarCommand)
    println(kraken.execute(deleteJarCommand))

    /** LOG OUT OF KRAKEN */
    kraken.exit
  }

  def getShellScript(jarname: String, mainClass: String, serializedConfiguration: String): String = {
    val script = """
#!/bin/bash
#PBS -N """ + jarname + """
#PBS -l nodes=1:ppn=24
#PBS -l walltime=604800,cput=2400000,mem=55000mb
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
mv ~/$jarname $working_dir/

# run test
cmd="java $vm_args -cp $working_dir/$jarname $mainClass $serializedConfiguration"
$cmd

# remove temporary directory
rm -rdf $working_dir
"""
    script
  }

}