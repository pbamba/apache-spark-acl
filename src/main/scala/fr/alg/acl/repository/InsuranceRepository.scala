package fr.alg.acl.repository

import fr.alg.acl.domain.Insurance
import org.apache.spark.sql.{Dataset, SparkSession}

class InsuranceRepository(sparkSession: SparkSession) extends Serializable {
  def load(): Dataset[Insurance] = {
    import sparkSession.implicits._
sparkSession.read.format("csv")
      .option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("insurance.csv").as[Insurance]
  }
}
