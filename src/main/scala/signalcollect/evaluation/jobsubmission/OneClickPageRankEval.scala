package signalcollect.evaluation.jobsubmission

import signalcollect.api._
import scala.util.Random
import signalcollect.evaluation.configuration._

/*
 * Packages the application, deploys the benchmarking jar/script to kraken
 * and then executes it via torque.
 * 
 * REQUIRES CERTIFICATE FOR LOGIN ON KRAKEN 
 */
object OneClickPageRankEval extends App {
  var krakenUsername = System.getProperty("user.name")
  if (args.size == 3) {
    krakenUsername = args(2)
  }
  val eval = new OneClickPageRankEval(args(0), args(1), krakenUsername)
  eval.executeEvaluation
}

class OneClickPageRankEval(gmailAccount: String, gmailPassword: String, krakenUsername: String = "stutz") extends OneClickEval(krakenUsername) {
  val computeGraphBuilders = List(DefaultBuilder, DefaultSynchronousBuilder)
  val numberOfRepetitions = 10
  val numberOfWorkers = (1 to 24).toList //List(24)
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