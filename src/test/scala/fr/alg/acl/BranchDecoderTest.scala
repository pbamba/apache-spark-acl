package fr.alg.acl

import com.holdenkarau.spark.testing.DatasetSuiteBase
import fr.alg.acl.domain.{Branch, LegacyAdministrativeDocument, TranslationError, TranslationStep}
import fr.alg.acl.repository.BranchRepository
import fr.alg.acl.translator.BranchDecoder
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.Checkers


class BranchDecoderTest extends FunSuite with Checkers with MockitoSugar with DatasetSuiteBase {

  test("something") {

    def mock[T <: AnyRef](implicit manifest: Manifest[T]): T = super.mock[T](withSettings().serializable())
    import spark.implicits._

    val legacyDomainObject: LegacyAdministrativeDocument = LegacyAdministrativeDocument("1234", 4321)
    val branch: Branch = Branch("1234", "branchName", "townName", "countryName")

    val mockBranchRepository = mock[BranchRepository]
    when(mockBranchRepository.load()).thenReturn(Seq(branch).toDS())

    val initialStep = TranslationStep(legacyDomainObject)
    val expectedFollowingStep = TranslationStep(legacyDomainObject, Option(Branch("1234", "branchName", "townName", "countryName")), None, List[TranslationError]())

    val branchDecoder = new BranchDecoder(spark, mockBranchRepository)

    val resultDS = branchDecoder.translate(Seq(initialStep).toDS())
    val expectedDS = Seq(expectedFollowingStep).toDS()

    assertDatasetEquals(expectedDS, resultDS)
  }

  test("something else") {

    def mock[T <: AnyRef](implicit manifest: Manifest[T]): T = super.mock[T](withSettings().serializable())
    import spark.implicits._

    val legacyDomainObject: LegacyAdministrativeDocument = LegacyAdministrativeDocument("1234", 4321)
    val branch: Branch = Branch("1235", "branchName", "townName", "countryName")

    val mockBranchRepository = mock[BranchRepository]
    when(mockBranchRepository.load()).thenReturn(Seq(branch).toDS())

    val initialStep = TranslationStep(legacyDomainObject)
    val expectedFollowingStep = TranslationStep(legacyDomainObject, None, None, List[TranslationError]() :+ TranslationError("Translator responsible for decoding branch information from free text"))

    val branchDecoder = new BranchDecoder(spark, mockBranchRepository)

    val resultDS = branchDecoder.translate(Seq(initialStep).toDS())
    val expectedDS = Seq(expectedFollowingStep).toDS()

    assertDatasetEquals(expectedDS, resultDS)
  }
}