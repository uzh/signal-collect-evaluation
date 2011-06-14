package signalcollect.evaluation.jobsubmission

import signalcollect.api._
import scala.util.Random
import signalcollect.evaluation.configuration._
import signalcollect.implementations.logging.DefaultLogger

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
  override lazy val jobDescription: String = "improvedGraphApi"
  override lazy val executionLocation = Kraken(System.getProperty("user.name")) //LocalHost

  lazy val computeGraphBuilders = List(DefaultBuilder, DefaultSynchronousBuilder) //List(DefaultSynchronousBuilder.withLogger(new DefaultLogger).withMessageBusFactory(Factory.MessageBus.Verbose))
  lazy val numberOfRepetitions = 3
  lazy val numberOfWorkers = List(1, 3, 6, 12, 18, 24) //(1 to 24).toList //List(24)

  def createConfigurations: List[Configuration] = {
    var configurations = List[Configuration]()
    for (computeGraphBuilder <- computeGraphBuilders) {
      for (workers <- numberOfWorkers) {
        for (repetition <- 1 to numberOfRepetitions) {
          val config = new PageRankConfiguration(
            spreadsheetConfiguration = Some(new SpreadsheetConfiguration(gmailAccount, gmailPassword, "evaluation", "data")),
            submittedByUser = System.getProperty("user.name"),
            builder = computeGraphBuilder.withNumberOfWorkers(workers),
            graphSize = 200000,
            jobId = Random.nextInt.abs,
            evaluationDescription = jobDescription)
          configurations = config :: configurations
        }
      }
    }
    configurations
  }
}