package fr.alg.acl.adapter

import fr.alg.acl.domain.{AdministrativeDocument, TranslationStep}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{Dataset, ForeachWriter, SparkSession}

import scalaj.http.{Http, HttpRequest}

class LegacyDomainStructuredStreamingAdapter(sparkSession: SparkSession) extends LegacyDomainAdapter(sparkSession) {

  override def write(successesDs: Dataset[AdministrativeDocument], errorsDs: Dataset[TranslationStep]): Unit = {
    val errorsQuery = errorsDs.writeStream.format("json").trigger(ProcessingTime(2000L))
      .option("checkpointLocation", "checkpoint1")
      .option("path", "errors").start()

    successesDs.writeStream.foreach(new ForeachWriter[AdministrativeDocument] {
      var httpRequest: HttpRequest = _
      var partitionId: Long  = _

      def open(partitionId: Long, version: Long): Boolean = {
        httpRequest = Http("http://httpbin.org/post")
        true
      }

      def process(record: AdministrativeDocument): Unit = {
        httpRequest.postData(record.toJson).asString
      }

      def close(errorOrNull: Throwable): Unit = {}
    })

    val successQuery = successesDs.writeStream.foreach(new ForeachWriter[AdministrativeDocument] {
      var httpRequest: HttpRequest = _
      var partitionId: Long  = _

      def open(partitionId: Long, version: Long): Boolean = {
        httpRequest = Http("http://httpbin.org/post")
        true
      }

      def process(record: AdministrativeDocument): Unit = {
        httpRequest.postData(record.toJson).asString
      }

      def close(errorOrNull: Throwable): Unit = {}
    }).start()

    successQuery.awaitTermination()
    errorsQuery.awaitTermination()
  }
}
