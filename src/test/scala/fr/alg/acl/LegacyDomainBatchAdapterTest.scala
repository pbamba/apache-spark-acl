package fr.alg.acl

import com.holdenkarau.spark.testing.DatasetSuiteBase
import fr.alg.acl.adapter.LegacyDomainBatchAdapter
import fr.alg.acl.domain._
import fr.alg.acl.repository.{BranchRepository, InsuranceRepository}
import fr.alg.acl.translator.{BranchDecoder, InsuranceResolver}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.Checkers

class LegacyDomainBatchAdapterTest extends FunSuite with Checkers with MockitoSugar with DatasetSuiteBase {

  test("something") {

    def mock[T <: AnyRef](implicit manifest: Manifest[T]): T = super.mock[T](withSettings().serializable())
    import spark.implicits._

    val legacyDomainObject: LegacyAdministrativeDocument = LegacyAdministrativeDocument("1234", 4321)
    val branch: Branch = Branch("1234", "branchName", "townName", "countryName")
    val insurance: Insurance = Insurance(4321, "insuranceName")

    val mockBranchRepository = mock[BranchRepository]
    when(mockBranchRepository.load()).thenReturn(Seq(branch).toDS())
    val mockInsuranceRepository = mock[InsuranceRepository]
    when(mockInsuranceRepository.load()).thenReturn(Seq(insurance).toDS())

    val inputDF = Seq(legacyDomainObject).toDF("branchCode", "insuranceId")
    val adapter = new LegacyDomainBatchAdapter(spark)

    val insuranceResolver = new InsuranceResolver(spark, mockInsuranceRepository)
    val branchDecoder = new BranchDecoder(spark, mockBranchRepository)

    val translationStepDS: Dataset[TranslationStep] = adapter.initialize(inputDF)
    val (resultSuccessDS, resultErrorDS) = adapter.adapt(translationStepDS, Seq(insuranceResolver, branchDecoder))
    val expectedDomainDS = Seq(AdministrativeDocument(branch, insurance)).toDS()

    assertDatasetEquals(expectedDomainDS, resultSuccessDS)
    assert(resultErrorDS.rdd.isEmpty())
    assert(inputDF.count() == (resultErrorDS.count() + resultSuccessDS.count()))
  }

  test("something") {

    def mock[T <: AnyRef](implicit manifest: Manifest[T]): T = super.mock[T](withSettings().serializable())
    import spark.implicits._

    val legacyDomainObject: LegacyAdministrativeDocument = LegacyAdministrativeDocument("1234", 4321)
    val branch: Branch = Branch("1234", "branchName", "townName", "countryName")
    val insurance: Insurance = Insurance(4321, "insuranceName")

    val mockBranchRepository = mock[BranchRepository]
    when(mockBranchRepository.load()).thenReturn(Seq(branch).toDS())
    val mockInsuranceRepository = mock[InsuranceRepository]
    when(mockInsuranceRepository.load()).thenReturn(Seq(insurance).toDS())

    val inputDF = Seq(legacyDomainObject).toDF("branchCode", "insuranceId")

    val adapter = new LegacyDomainBatchAdapter(spark)
    val insuranceResolver = new InsuranceResolver(spark, mockInsuranceRepository)
    val branchDecoder = new BranchDecoder(spark, mockBranchRepository)
    val resultDs = adapter.adapt(adapter.initialize(inputDF), Seq(insuranceResolver, branchDecoder))
    val expectedDomainDS = Seq(AdministrativeDocument(branch, insurance)).toDS()

    assertDatasetEquals(expectedDomainDS, resultDs)
    assert(inputDF.count() == resultDs.count())
  }
}
