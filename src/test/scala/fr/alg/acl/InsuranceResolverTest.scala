package fr.alg.acl

import com.holdenkarau.spark.testing.DatasetSuiteBase
import fr.alg.acl.domain.{Insurance, LegacyAdministrativeDocument, TranslationError, TranslationStep}
import fr.alg.acl.repository.InsuranceRepository
import fr.alg.acl.translator.InsuranceResolver
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.Checkers


class InsuranceResolverTest extends FunSuite with Checkers with MockitoSugar with DatasetSuiteBase {

  test("something") {

    def mock[T <: AnyRef](implicit manifest: Manifest[T]): T = super.mock[T](withSettings().serializable())
    import spark.implicits._

    val legacyDomainObject: LegacyAdministrativeDocument = LegacyAdministrativeDocument("1234", 4321)
    val insurance: Insurance = Insurance(4321, "insuranceName")

    val mockInsuranceRepository = mock[InsuranceRepository]
    when(mockInsuranceRepository.load()).thenReturn(Seq(insurance).toDS())

    val initialStep = TranslationStep(legacyDomainObject)
    val expectedFollowingStep = TranslationStep(legacyDomainObject, None, Option(Insurance(4321, "insuranceName")), List[TranslationError]())

    val insuranceResolver = new InsuranceResolver(spark, mockInsuranceRepository)

    val resultDS = insuranceResolver.translate(Seq(initialStep).toDS())
    val expectedDS = Seq(expectedFollowingStep).toDS()

    assertDatasetEquals(expectedDS, resultDS)
  }

  test("something else") {

    def mock[T <: AnyRef](implicit manifest: Manifest[T]): T = super.mock[T](withSettings().serializable())
    import spark.implicits._

    val legacyDomainObject: LegacyAdministrativeDocument = LegacyAdministrativeDocument("1234", 4321)
    val insurance: Insurance = Insurance(4322, "insuranceName")

    val mockInsuranceRepository = mock[InsuranceRepository]
    when(mockInsuranceRepository.load()).thenReturn(Seq(insurance).toDS())

    val initialStep = TranslationStep(legacyDomainObject)
    val expectedFollowingStep = TranslationStep(legacyDomainObject, None, None, List[TranslationError]() :+ TranslationError("Translator responsible for adding insurance information"))

    val insuranceResolver = new InsuranceResolver(spark, mockInsuranceRepository)

    val resultDS = insuranceResolver.translate(Seq(initialStep).toDS())
    val expectedDS = Seq(expectedFollowingStep).toDS()

    assertDatasetEquals(expectedDS, resultDS)
  }
}