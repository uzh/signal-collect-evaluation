package signalcollect.evaluation.configuration

class JobConfiguration(
  var jobId: Int,
  var spreadsheetConfiguration: Option[SpreadsheetConfiguration],
  var submittedByUser: String,
  var evaluationDescription: String) extends Serializable