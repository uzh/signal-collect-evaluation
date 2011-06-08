package signalcollect.evaluation.configuration

import signalcollect.api.ComputeGraphBuilder

class PageRankConfiguration(
  jobId: Int,
  gmailAccount: String,
  gmailPassword: String,
  spreadsheetName: String,
  worksheetName: String,
  submittedByUser: String,
  evaluationDescription: String,
  var builder: ComputeGraphBuilder,
  var graphSize: Int) extends Configuration(jobId, gmailAccount, gmailPassword, spreadsheetName, worksheetName, submittedByUser, evaluationDescription)