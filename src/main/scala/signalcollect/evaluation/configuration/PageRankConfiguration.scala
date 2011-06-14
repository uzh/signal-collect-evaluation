package signalcollect.evaluation.configuration

import signalcollect.api.ComputeGraphBuilder

class PageRankConfiguration(
  jobId: Int,
  spreadsheetConfiguration: Option[SpreadsheetConfiguration] = None,
  submittedByUser: String,
  evaluationDescription: String,
  var builder: ComputeGraphBuilder,
  var graphSize: Int) extends Configuration(jobId, spreadsheetConfiguration, submittedByUser, evaluationDescription)