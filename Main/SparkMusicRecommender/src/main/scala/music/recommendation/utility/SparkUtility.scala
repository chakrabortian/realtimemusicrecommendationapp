package music.recommendation.utility

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession

/**
  * Created by deveshkandpal on 4/20/17.
  */
object SparkUtility {

  val spark = getSparkSession

  def getSparkSession : SparkSession = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "7g")
      .config("spark.executor.memory", "3g")
      .getOrCreate()

    spark

  }

  val kafkaParams =  Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test-consumer-group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

}
