package fr.alg.acl.translator

import fr.alg.acl.domain.TranslationStep
import org.apache.spark.sql.Dataset

trait Translator {
  val description: String
  val translate: Dataset[TranslationStep] => Dataset[TranslationStep]
}
