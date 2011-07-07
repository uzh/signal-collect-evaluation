package signalcollect.evaluation.configuration

import signalcollect.configuration.ComputeGraphBuilder
import signalcollect.configuration.ExecutionConfiguration

class PageRankConfiguration(
  jobId: Int,
  spreadsheetConfiguration: Option[SpreadsheetConfiguration] = None,
  submittedByUser: String,
  evaluationDescription: String,
  val builder: ComputeGraphBuilder,
  val executionConfiguration: ExecutionConfiguration,
  val graphSize: Int) extends JobConfiguration(jobId, spreadsheetConfiguration, submittedByUser, evaluationDescription)