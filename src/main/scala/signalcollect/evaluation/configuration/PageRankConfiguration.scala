package signalcollect.evaluation.configuration

import signalcollect.configuration.ComputeGraphBuilder

class PageRankConfiguration(
  jobId: Int,
  spreadsheetConfiguration: Option[SpreadsheetConfiguration] = None,
  submittedByUser: String,
  evaluationDescription: String,
  var builder: ComputeGraphBuilder,
  var graphSize: Int) extends JobConfiguration(jobId, spreadsheetConfiguration, submittedByUser, evaluationDescription)