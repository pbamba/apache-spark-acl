package fr.alg.acl.service

import fr.alg.acl.adapter.LegacyDomainStructuredStreamingAdapter
import fr.alg.acl.repository.{BranchRepository, InsuranceRepository}
import fr.alg.acl.translator.{BranchDecoder, InsuranceResolver}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object LegacyDomainStreamingService {

  def main(arg: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Legacy Domain Streaming Service")
      .getOrCreate()

    val inputDF = sparkSession.readStream
      .format("redis")
      .schema(StructType(StructField("branchCode", StringType) :: StructField("insuranceId", IntegerType) :: Nil))
      .load()

    val insuranceResolver = new InsuranceResolver(sparkSession, new InsuranceRepository(sparkSession))
    val branchDecoder = new BranchDecoder(sparkSession, new BranchRepository(sparkSession))

    val adapter = new LegacyDomainStructuredStreamingAdapter(sparkSession)
    val translationStepDS = adapter.initialize(inputDF)
    val (successes, errors) = adapter.adapt(translationStepDS, Seq(insuranceResolver, branchDecoder))
    adapter.write(successes, errors)
  }
}
