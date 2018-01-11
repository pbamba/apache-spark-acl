package fr.alg.acl.translator
import fr.alg.acl.domain.{TranslationError, TranslationStep}
import fr.alg.acl.repository.InsuranceRepository
import org.apache.spark.sql.{Dataset, SparkSession}

class InsuranceResolver(sparkSession: SparkSession, insuranceRepository: InsuranceRepository)
  extends Translator with Serializable {

  override val description: String = "Translator responsible for adding insurance information"

  override val translate: Dataset[TranslationStep] => Dataset[TranslationStep] = {
    (stepDs) =>
      import sparkSession.implicits._
      val insuranceDs = insuranceRepository.load()
      stepDs.joinWith(insuranceDs, $"legacyAdministrativeDocument.insuranceId" === $"id", "left").map {
        case (step, null) =>
          step.copy(translationErrors = step.translationErrors :+ TranslationError(description))
        case (step, insurance) =>  step.copy(insurance = Option(insurance))
      }
  }
}
