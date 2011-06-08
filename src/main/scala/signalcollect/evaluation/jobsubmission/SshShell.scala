/*
 *  @author Philip Stutz
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

import java.io.File
import java.io.ByteArrayInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import ch.ethz.ssh2.Connection
import ch.ethz.ssh2.StreamGobbler

class SshShell(
  username: String = System.getProperty("user.name"),
  hostname: String = "kraken.ifi.uzh.ch",
  port: Int = 22,
  privateKeyFilePath: String = System.getProperty("user.home") + System.getProperty("file.separator") + ".ssh" + System.getProperty("file.separator") + "id_rsa") {

  var connection = new Connection(hostname, port)
  connection.connect
  connection.authenticateWithPublicKey(username, new File(privateKeyFilePath), null)

  def execute(command: String): String = {
    val session = connection.openSession
    session.execCommand(command)
    IoUtil.streamToString(new StreamGobbler(session.getStdout))
  }

  def exit {
    connection.close
  }

}