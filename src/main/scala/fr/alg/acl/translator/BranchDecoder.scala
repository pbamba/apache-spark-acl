package fr.alg.acl.translator

import fr.alg.acl.domain.{TranslationError, TranslationStep}
import fr.alg.acl.repository.BranchRepository
import org.apache.spark.sql.{Dataset, SparkSession}


class BranchDecoder(sparkSession: SparkSession, branchRepository: BranchRepository) extends Translator with Serializable {

  override val description: String = "Translator responsible for decoding branch information from free text"

  override val translate: Dataset[TranslationStep] => Dataset[TranslationStep] = {
    (stepDs) =>
      import sparkSession.implicits._

      val branchDs = branchRepository.load()
      stepDs.joinWith(branchDs, $"legacyAdministrativeDocument.branchCode" === $"code", "left").map {
        case (step, null) =>
          step.copy(translationErrors = step.translationErrors :+ TranslationError(description))
        case (step, branch) => step.copy(branch = Option(branch))
      }
  }
}
