package fr.alg.acl.source

import com.redislabs.provider.redis._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

class RedisSource(sparkSession: SparkSession,
                  override val schema: StructType) extends Source {

  protected var lastOffsetCommitted: LongOffset = new LongOffset(0)

  override def getOffset: Option[Offset] = {
    val sortedScoreRDD = sparkSession.sparkContext
      .fromRedisZRangeByScoreWithScore("*", lastOffsetCommitted.offset.toDouble, Double.MaxValue)
      .map(_._2).sortBy(-_)

    if(sortedScoreRDD.count() == 0){
      None
    }  else {
      Some(LongOffset(sortedScoreRDD.first().toLong))
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startPos = start.flatMap(LongOffset.convert).getOrElse(LongOffset(0)).offset.toInt
    val endPos = LongOffset.convert(end).getOrElse(LongOffset(0)).offset.toInt

    import sparkSession.implicits._

    val jsonDataset: Dataset[String] = sparkSession.sparkContext.fromRedisZRangeByScore("*", startPos, endPos).toDS()
    sparkSession.sqlContext.read.schema(schema).json(jsonDataset)
  }

  override def commit(end: Offset) : Unit = {
    lastOffsetCommitted = LongOffset.convert(end).getOrElse(LongOffset(0))
  }

  override def stop() {}
}

class RedisSourceProvider extends StreamSourceProvider with DataSourceRegister {
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) =
    (shortName(), schema.getOrElse(StructType(Nil)))

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {
    new RedisSource(sqlContext.sparkSession, schema.getOrElse(StructType(Nil)))
  }

  override def shortName(): String = "redis"
}
