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

package com.signalcollect.evaluation.resulthandling

import java.net.URL
import scala.collection.JavaConversions._
import com.google.gdata.client.spreadsheet._
import com.google.gdata.data._
import com.google.gdata.data.spreadsheet._

class GoogleDocsResultHandler(username: String, password: String, spreadsheetName: String, worksheetName: String) extends ResultHandler {
  

  def addEntry(data: Map[String, String]) = {
    val service = new SpreadsheetService("uzh-signalcollect-2.0.0")
  service.setUserCredentials(username, password)
    val spreadsheet = getSpreadsheet(spreadsheetName, service)
    val worksheet = getWorksheetInSpreadsheet(worksheetName, spreadsheet)
    insertRow(worksheet, data, service)
  }

  def getWorksheetInSpreadsheet(title: String, spreadsheet: SpreadsheetEntry): WorksheetEntry = {
    var result: WorksheetEntry = null.asInstanceOf[WorksheetEntry]
    val worksheets = spreadsheet.getWorksheets
    for (worksheet <- worksheets) {
      val currentWorksheetTitle = worksheet.getTitle.getPlainText
      if (currentWorksheetTitle == title) {
        result = worksheet
      }
    }
    if (result == null) {
      throw new Exception("Worksheet with title \"" + title + "\" not found within spreadsheet " + spreadsheet.getTitle.getPlainText + ".")
    }
    result
  }

  def getSpreadsheet(title: String, service: SpreadsheetService): SpreadsheetEntry = {
    var result: SpreadsheetEntry = null.asInstanceOf[SpreadsheetEntry]
    val spreadsheetFeedUrl = new URL("https://spreadsheets.google.com/feeds/spreadsheets/private/full")
    val spreadsheetFeed = service.getFeed(spreadsheetFeedUrl, classOf[SpreadsheetFeed])
    val spreadsheets = spreadsheetFeed.getEntries
    for (spreadsheet <- spreadsheets) {
      val currentSpreadsheetTitle = spreadsheet.getTitle.getPlainText
      if (currentSpreadsheetTitle == title) {
        result = spreadsheet
      }
    }
    if (result == null) {
      throw new Exception("Spreadsheet with title \"" + title + "\" not found.")
    }
    result
  }

  def insertRow(worksheet: WorksheetEntry, dataMap: Map[String, String], service: SpreadsheetService) {
    val newEntry = new ListEntry
    val elem = newEntry.getCustomElements
    for (dataTuple <- dataMap) {
      elem.setValueLocal(dataTuple._1, dataTuple._2)
    }
    service.insert(worksheet.getListFeedUrl, newEntry)
  }
}