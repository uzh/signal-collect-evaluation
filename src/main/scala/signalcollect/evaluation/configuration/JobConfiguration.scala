package signalcollect.evaluation.configuration

import signalcollect.api.ComputeGraphBuilder

class JobConfiguration(
  var jobId: Int,
  var spreadsheetConfiguration: Option[SpreadsheetConfiguration],
  var submittedByUser: String,
  var evaluationDescription: String) extends Serializable