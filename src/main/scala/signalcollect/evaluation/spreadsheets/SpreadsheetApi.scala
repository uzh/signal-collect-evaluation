package signalcollect.evaluation.spreadsheets

import java.net.URL
import scala.collection.JavaConversions._
import com.google.gdata.client.spreadsheet._
import com.google.gdata.data._
import com.google.gdata.data.spreadsheet._

class SpreadsheetApi(
  gmailAccount: String,
  gmailPassword: String) {

  val service = new SpreadsheetService("uzh-signalcollect-0.0.1")
  service.setUserCredentials(gmailAccount, gmailPassword)

  val evaluationSpreadsheet = getSpreadsheet("evaluation")
  val dataWorksheet = getWorksheetInSpreadsheet("data", evaluationSpreadsheet)

  def insertRow(worksheet: WorksheetEntry, dataMap: Map[String, String]) {
    val newEntry = new ListEntry
    val elem = newEntry.getCustomElements
    for (dataTuple <- dataMap) {
      elem.setValueLocal(dataTuple._1, dataTuple._2)
    }
    service.insert(worksheet.getListFeedUrl, newEntry)
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

  def getSpreadsheet(title: String): SpreadsheetEntry = {
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

}