package fr.alg.acl.adapter

import fr.alg.acl.domain.{AdministrativeDocument, TranslationStep}
import org.apache.spark.sql.{Dataset, SparkSession}

import scalaj.http.Http

class LegacyDomainBatchAdapter(sparkSession: SparkSession) extends LegacyDomainAdapter(sparkSession) {

  override def write(successesDs: Dataset[AdministrativeDocument], errorsDs: Dataset[TranslationStep]): Unit = {

    successesDs.foreachPartition { partitionOfRecords =>
      val httpRequest = Http("http://httpbin.org/post")
      partitionOfRecords.foreach { record =>
        httpRequest.postData(record.toJson).asString
      }
    }

    errorsDs.write.json("apache-spark-acl/batch_error")
  }
}
