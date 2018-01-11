package fr.alg.acl.adapter

import fr.alg.acl.domain.{AdministrativeDocument, LegacyAdministrativeDocument, TranslationStep}
import fr.alg.acl.translator.Translator
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

abstract class LegacyDomainAdapter(sparkSession: SparkSession) {

  import sparkSession.implicits._

  def initialize(inputDF: DataFrame): Dataset[TranslationStep] = {
    inputDF.as[LegacyAdministrativeDocument].map(TranslationStep(_))
  }

  def adapt(translationStepDs: Dataset[TranslationStep], translators: Seq[Translator]): (Dataset[AdministrativeDocument],Dataset[TranslationStep]) = {
    val translations: Seq[Dataset[TranslationStep] => Dataset[TranslationStep]] = translators.map(_.translate)
    val pipeline: Dataset[TranslationStep] => Dataset[TranslationStep] = translations.reduceLeft(_ andThen _)

    val (successesDS, errorsDS) = filterResults(translationStepDs.transform(pipeline))
    (successesDS, errorsDS)
  }

  protected def filterResults(resultsDs: Dataset[TranslationStep]):
  (Dataset[AdministrativeDocument], Dataset[TranslationStep]) = {
    val errorsDS: Dataset[TranslationStep] = resultsDs.filter(_.translationErrors.nonEmpty)
    val successesDS: Dataset[AdministrativeDocument] = resultsDs.filter(_.translationErrors.isEmpty)
      .map(AdministrativeDocument(_))
    (successesDS, errorsDS)
  }

  def write(successesDS: Dataset[AdministrativeDocument],
                      errorsDS: Dataset[TranslationStep]): Unit
}
