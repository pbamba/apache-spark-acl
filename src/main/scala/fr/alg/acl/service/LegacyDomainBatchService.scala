package fr.alg.acl.service

import fr.alg.acl.adapter.LegacyDomainBatchAdapter
import fr.alg.acl.repository.{BranchRepository, InsuranceRepository}
import fr.alg.acl.translator.{BranchDecoder, InsuranceResolver}
import org.apache.spark.sql.SparkSession

object LegacyDomainBatchService {

  def main(arg: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Legacy Domain Batch Service")
      .getOrCreate()

    val inputDF = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load("legacy_administrative_document.csv")

    val insuranceResolver = new InsuranceResolver(sparkSession, new InsuranceRepository(sparkSession))
    val branchDecoder = new BranchDecoder(sparkSession, new BranchRepository(sparkSession))

    val adapter = new LegacyDomainBatchAdapter(sparkSession)
    val translationStepDS = adapter.initialize(inputDF)
    val (successes, errors) = adapter.adapt(translationStepDS, Seq(insuranceResolver, branchDecoder))
    adapter.write(successes, errors)
  }
}
