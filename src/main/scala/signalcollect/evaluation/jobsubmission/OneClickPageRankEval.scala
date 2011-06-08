package signalcollect.evaluation.jobsubmission

import signalcollect.api.DefaultBuilder
import scala.util.Random
import signalcollect.evaluation.configuration._

/*
 * Packages the application, deploys the benchmarking jar/script to kraken
 * and then executes it via torque.
 * 
 * REQUIRES CERTIFICATE FOR LOGIN ON KRAKEN 
 */
object OneClickPageRankEval extends App {
  val eval = new OneClickPageRankEval(args(0), args(1))
  eval.executeEvaluation
}

class OneClickPageRankEval(gmailAccount: String, gmailPassword: String) extends OneClickEval {
  val computeGraphBuilders = List(DefaultBuilder) //, DefaultSynchronousBuilder)
  val numberOfRepetitions = 1
  val numberOfWorkers = List(24)
  def createConfigurations: List[Configuration] = {
    var configurations = List[Configuration]()
    for (computeGraphBuilder <- computeGraphBuilders) {
      for (workers <- numberOfWorkers) {
        for (repetition <- 1 to numberOfRepetitions) {
          val config = new PageRankConfiguration(
            gmailAccount = gmailAccount,
            gmailPassword = gmailPassword,
            spreadsheetName = "evaluation",
            worksheetName = "data",
            submittedByUser = System.getProperty("user.name"),
            builder = computeGraphBuilder.withNumberOfWorkers(workers),
            graphSize = 10000,
            jobId = Random.nextInt,
            evaluationDescription = "just toying")
          configurations = config :: configurations
        }
      }
    }
    configurations
  }
}