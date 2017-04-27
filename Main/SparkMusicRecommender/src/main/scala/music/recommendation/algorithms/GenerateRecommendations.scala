package music.recommendation.algorithms

import java.util.Properties
import java.util.concurrent.Future

import music.recommendation.bo.{LyricsInfo, SongVectorPrediction}
import music.recommendation.dao.MongoOperations
import music.recommendation.utility.FileLocations.ALS_MODEL_TRAIN_LOCATION
import music.recommendation.utility.SparkUtility
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer._
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.immutable.Iterable

/**
  * Created by deveshkandpal on 4/18/17.
  */
class GenerateRecommendations {

  private def getDistance(vector: Vector,clusterCentre: Vector): Double = {
    val centreArray = clusterCentre.toArray
    val vectorArray = vector.toArray
    val p = centreArray.indices map( i => {

      (centreArray(i) - vectorArray(i)) * (centreArray(i) - vectorArray(i) )
    })
    Math.sqrt(p.toList.reduce(_ + _))

  }

  def handleSongRecomendation(record : ConsumerRecord[String, String], lyricsUsersClusters : KMeansModel, lyricsSongsTuple : (KMeansModel,List[SongVectorPrediction]), lyricsWithRelevantPosList : List[LyricsInfo]): Future[RecordMetadata] ={
    val userId = record.key
    val userPreference = record.value.split("<SEP>").toList
    val lyricsSongsClusters = lyricsSongsTuple._1
    val songPrediction = lyricsSongsTuple._2
    val predictedCluster = lyricsSongsClusters.predict(getVectorsForUserSession(userPreference, lyricsWithRelevantPosList).head)
    val clusterCentre = lyricsSongsClusters.clusterCenters(predictedCluster)
    val recommendations = songPrediction.filter(sp => sp.prediction == predictedCluster).map(s => (s.id,getDistance(s.vector,clusterCentre))).sortWith((rec1, rec2) => rec1._2 > rec2._2).map(sv => sv._1).take(10).mkString("<SEP>")
    pushToKafka(userId, "SongRecoResponse", recommendations)
  }

  def handleExistingUserRecomendation(record : ConsumerRecord[String, String],artistZip : Map[String, Long], userZip : Map[String , Long]): Future[RecordMetadata] = {
    println("Starting artist recommendaton => key : " + record.key() + " value : " + record.value)
    val reversedArtistZip = artistZip.map(a => (a._2, a._1))
    val userId = record.key
    val mongoOps = new MongoOperations()
    val predictedCluster = mongoOps.getExistingUserPrediction(userId).toInt
    println("*** LOADING ALS MODEL *** WITH PREDICTED CLUSTER" + predictedCluster)
    val predictedALSModel = MatrixFactorizationModel.load(SparkUtility.spark.sparkContext, ALS_MODEL_TRAIN_LOCATION + predictedCluster)
    println("user name : " + userId + " and corresponding id : " + userZip(userId))
    val ratingRecomendition = predictedALSModel.recommendProducts(userZip(userId).toInt,5)
    val artistRecomendition = ratingRecomendition.map(ar => reversedArtistZip(ar.product)).mkString("<SEP>")
    pushToKafka(userId, "ExistingUserRecoResponse", artistRecomendition)

  }

  def handleColdStartRecommendation(record : ConsumerRecord[String, String],
                                 clusters : KMeansModel, artistZip : Map[String, Long]): Future[RecordMetadata] = {

    println("Starting artist recommendaton => key : " + record.key() + " value : " + record.value())
    val userId = record.key
    val choices = record.value.split("<SEP>")
    val predictedCluster = clusters.predict(Vectors.dense(Array[Double
      ](artistZip(choices(0)).toDouble, artistZip(choices(1)).toDouble, artistZip(choices(2)).toDouble)))
    println("Cold start user : " + userId + " has following choice => " + choices.mkString("::"))
    println("Cold start user belongs to cluster id :  " + predictedCluster)
    val mongoOps = new MongoOperations()
    mongoOps.saveNewUserPrediction(userId, predictedCluster)
    val recommendations = handleALSRecommendations(userId,choices,artistZip,predictedCluster)
    pushToKafka(userId, "ColdStartRecoResponse",recommendations)
  }

  def handleArtistForArtistRecomendation(record : ConsumerRecord[String, String],
                                        clusters : KMeansModel, artistZip : Map[String, Long]): Future[RecordMetadata] = {
    println("Starting artist recommendaton => key : " + record.key() + " value : " + record.value())
    val userId = record.key
    val choices = record.value.split("<SEP>")
    val predictedCluster = clusters.predict(Vectors.dense(Array[Double
      ](artistZip(choices(0)).toDouble, artistZip(choices(1)).toDouble, artistZip(choices(2)).toDouble)))
    val recommendations = handleALSRecommendations(userId,choices,artistZip,predictedCluster)
    pushToKafka(userId, "ArtistforArtistRecoResponse",recommendations)
  }

  private def handleALSRecommendations(userId:String, choices : Array[String], artistZip : Map[String, Long],predictedCluster : Int ) : String = {
    val artistReversedZip = artistZip.map(m => (m._2, m._1))

    println("*** LOADING ALS MODEL ***")

    val predictedALSModel = MatrixFactorizationModel.load(SparkUtility.spark.sparkContext, ALS_MODEL_TRAIN_LOCATION + predictedCluster)

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

    val recoString = finalRecos.mkString("<SEP>")
    recoString
  }

  private def pushToKafka(userId : String, topic : String, message : String): Future[RecordMetadata] = {

    val producer = GenerateRecommendations.producer
    val record = new ProducerRecord(topic, userId, message)
    producer.send(record)

  }

  private def getVectorsForUserSession(trackIds : List[String], lyricsWithRelevantPosList : List[LyricsInfo]): Iterable[Vector] = {

    val filteredLyricsInfo = lyricsWithRelevantPosList
      .filter( rdd => trackIds.contains(rdd.trackId))
      .groupBy(us2 => us2.pos).map(us3 => us3._2
      .sortWith((l1, l2) => l1.count > l2.count)
      .take(2))
      .flatMap(fm => fm)
      .groupBy(gb => gb.pos).map(mv => {
      if(mv._2.size < 2) {
        val zerosToBeAdded = 2 - mv._2.size
        val zerosList = List.fill(zerosToBeAdded)(LyricsInfo("",0,0,mv._1))
        val newMv = List.concat(mv._2, zerosList)
        (mv._1, newMv)
      } else (mv._1, mv._2)
    })

    filteredLyricsInfo.map(fli => Vectors.dense(fli._2.map(f => f.wordId.toDouble).toArray))

  }
}

object GenerateRecommendations {

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("acks","1")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val instance = new GenerateRecommendations()

  def getDistance(vector: Vector,clusterCentre: Vector): Double = instance.getDistance(vector, clusterCentre)

}


