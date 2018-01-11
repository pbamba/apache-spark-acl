package fr.alg.acl.repository

import fr.alg.acl.domain.Branch
import org.apache.spark.sql.{Dataset, SparkSession}

class BranchRepository(sparkSession: SparkSession) extends Serializable {

  def load(): Dataset[Branch] = {
    import sparkSession.implicits._


    println("")
    println("BRANCH REPOSITORY")
    println("")
    sparkSession.read.format("csv")
      .option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/Users/patrick/Digital/repos/apache-spark-acl/branch.csv").as[Branch]
  }
}
