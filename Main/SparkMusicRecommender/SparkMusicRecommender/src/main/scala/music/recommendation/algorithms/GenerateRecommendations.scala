package music.recommendation.algorithms

import music.recommendation.utility.FileLocations
import music.recommendation.utility.FileLocations.ALS_MODEL_TRAIN_LOCATION
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession

import scala.collection.Map

/**
  * Created by deveshkandpal on 4/18/17.
  */
class GenerateRecommendations {

  def handleArtistRecommendation(record : ConsumerRecord[String, String],
                                 clusters : KMeansModel, artistZip : Map[String, Long])  = {

    val artistReversedZip = artistZip.map(m => (m._2 , m._1))
    println("Starting artist recommendaton => key : " + record.key() + " value : " + record.value())
    val userId = record.key
    val valueArr =  record.value.split("<SEP>")
    // check if user is coldstart or old
    val userType =  valueArr(0)
    val choices = valueArr.drop(1)

    val predictedCluster = clusters.predict(Vectors.dense(Array[Double
      ](artistZip(choices(0)).toDouble, artistZip(choices(1)).toDouble, artistZip(choices(2)).toDouble)))
    println("Cold start user : "+ userId + " has following choice => " + choices.mkString("::"))
    println("Cold start user belongs to cluster id :  " + predictedCluster)

    println("*** LOADING ALS MODEL ***")

    val predictedALSModel = MatrixFactorizationModel.load(FileLocations.spark.sparkContext, ALS_MODEL_TRAIN_LOCATION + predictedCluster)

    val userList = choices.flatMap(choice => {
      predictedALSModel.recommendUsers(artistZip(choice).toInt, 5)
    }).toList

    val recommendations = userList.flatMap(u => {
      val recoUser = predictedALSModel.recommendProducts(u.user, 5).toList
      recoUser
    }).sortWith((rec1, rec2) => rec1.rating > rec2.rating)

    val uniqueRecos = recommendations.map(r => r.product).toSet

    val finalRecos = uniqueRecos.filter(ur => !choices.contains(artistReversedZip(ur))).map(m => artistReversedZip(m))

    finalRecos.foreach(r => {

      println(" user recommendations : " + r)
    })

    pushToKafka(record.key(), "generatedartistrecommendations", finalRecos.mkString("<SEP>"))

  }

  def pushToKafka(userId : String, topic : String, message : String) = {

    import java.util.Properties

    import org.apache.kafka.clients.producer._

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks","1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val record = new ProducerRecord(topic, userId, message)
    producer.send(record)

  }

}


