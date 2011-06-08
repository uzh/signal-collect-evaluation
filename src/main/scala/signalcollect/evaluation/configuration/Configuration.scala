package signalcollect.evaluation.configuration

import signalcollect.api.ComputeGraphBuilder

class Configuration(
  var jobId: Int,
  var gmailAccount: String,
  var gmailPassword: String,
  var spreadsheetName: String,
  var worksheetName: String,
  var submittedByUser: String,
  var evaluationDescription: String) extends Serializable