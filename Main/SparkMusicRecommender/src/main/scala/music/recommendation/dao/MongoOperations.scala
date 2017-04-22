package music.recommendation.dao

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import music.recommendation.bo.{ArtistCount, EntityPrediction, SongInfo}
import music.recommendation.utility.SparkUtility
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.rdd.RDD
import org.bson.Document

/**
  * Created by deveshkandpal on 4/20/17.
  */
class MongoOperations {


  def saveUserArtistInfo(combinedRdd : RDD[ArtistCount]): Unit = {

    val filteredUserArtist = combinedRdd
      .groupBy(ua => ua.userId)
      .filter(users => users._2.size >= 3)
        .mapValues(mv => mv.map(a => a.artist))

    val documents = filteredUserArtist.map(m => {
      val userId = m._1
      val artist = StringEscapeUtils.escapeXml(m._2.toList.mkString("<SEP>"))
      println(s"{user : '$userId', artist : '$artist'}")
      val doc = Document.parse(s"{user : '$userId', artist : '$artist'}")
      doc
    })

    MongoSpark.save(documents, MongoOperations.userArtistConfig)

    println("SAVED USER-ARTIST INFO TO MONGO DB SUCCESSFULLY!")

  }

  def saveArtistSongInfo(artistTrackRDD : RDD[SongInfo]): Unit = {

    val filteredUserArtist = artistTrackRDD
      .groupBy(ua => ua.artist)
      .mapValues(mv => mv.map(a => a.trackId +"<SONGINFO>"+a.songId+"<SONGINFO>"+a.title))

    val documents = filteredUserArtist.map(m => {
      val artist = StringEscapeUtils.escapeXml(m._1)
      val songs = StringEscapeUtils.escapeXml(m._2.toList.mkString("<SEP>"))
      val doc = Document.parse(s"{artist : '$artist', songs : '$songs'}")
      doc
    })

    MongoSpark.save(documents, MongoOperations.artistSongConfig)

  }

  def saveUserClusterPredictions(userPrediction : RDD[EntityPrediction]): Unit = {



    val documents = userPrediction.map(m => {
      val userId = m.id
      val prediction = m.prediction
      val doc = Document.parse(s"{user : '$userId', prediction : $prediction}")
      doc
    })

    MongoSpark.save(documents, MongoOperations.userPredictionConfig)

  }

  def saveNewUserPrediction(userId: String, prediction: Int): Unit = {
    val doc = List(Document.parse(s"{user : '$userId', prediction : $prediction}"))
    val sc = SparkUtility.spark.sparkContext
    val document = sc.parallelize(doc)
    MongoSpark.save(document, MongoOperations.userPredictionConfig)
  }

  def getExistingUserPrediction(userId: String): String = {
    val sc = SparkUtility.spark.sparkContext
    val documentRDD = MongoSpark.load(sc,MongoOperations.userPredictionReadConfig)
    val prediction = documentRDD.filter(up => up.toJson.contains(userId)).collect()(0).get("prediction").toString
    prediction
  }


}

object MongoOperations {

  val userArtistConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/musicrecoapp.userartist"))
  val artistSongConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/musicrecoapp.artistsong"))
  val userPredictionConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/musicrecoapp.userPrediction"))
  val userPredictionReadConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/musicrecoapp.userPrediction"))
}
