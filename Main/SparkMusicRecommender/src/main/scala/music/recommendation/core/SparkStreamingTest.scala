package music.recommendation.core

import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by anindita on 4/18/17.
  */
object SparkStreamingTest {

  def main(args : Array[String]) : Unit = {



    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local")

//    val spark: SparkSession = SparkSession
//      .builder().config(sparkConf)
//      .master("local")
//      .appName("Real Time Music Reco App")
//      .config("spark.driver.memory", "7g")
//      .config("spark.executor.memory", "3g")
//      .getOrCreate()



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )



    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topics = Array("kafkatopic")


    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val c = stream.map(record => (record.value).toString)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)


  }

}
