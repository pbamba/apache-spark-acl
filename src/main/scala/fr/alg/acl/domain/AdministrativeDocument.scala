package fr.alg.acl.domain

case class AdministrativeDocument(branch: Branch, insurance: Insurance) extends Jsonable

object AdministrativeDocument {
  def apply(translationStep: TranslationStep): AdministrativeDocument =
    new AdministrativeDocument(translationStep.branch.get, translationStep.insurance.get)
}


