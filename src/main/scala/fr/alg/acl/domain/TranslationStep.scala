package fr.alg.acl.domain



case class TranslationStep(
                            legacyAdministrativeDocument: LegacyAdministrativeDocument,
                            branch: Option[Branch],
                            insurance: Option[Insurance],
                            translationErrors: List[TranslationError]) extends Jsonable

object TranslationStep {
  def apply(legacyDomainObject: LegacyAdministrativeDocument): TranslationStep =
    new TranslationStep(legacyDomainObject, None, None, List[TranslationError]())
}
