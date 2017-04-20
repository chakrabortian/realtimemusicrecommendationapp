package music.recommendation.algorithms

import music.recommendation.algorithms.ArtistRecommendation
import music.recommendation.bo._
import music.recommendation.ingestion.DataInjestion
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import music.recommendation.utility.NLPUtility
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import music.recommendation.utility.FileLocations._

import scala.util.{Failure, Success, Try}

/**
  * Created by deveshkandpal on 4/18/17.
  */
class LyricsRecommendation {

  def prepareLyricsRecommendationModels(spark : SparkSession,
                                  lyricsWithRelevantPosRdd : RDD[LyricsInfo],
                                 userTasteDf : DataFrame, artistTrackDf : DataFrame) : (KMeansModel, KMeansModel) = {

    import spark.implicits._


    lyricsWithRelevantPosRdd.toDF().createOrReplaceTempView("lyricsInfo")
    userTasteDf.createOrReplaceTempView("userTaste")
    artistTrackDf.createOrReplaceTempView("songInfo")

    val combinedView = spark.sql("select u.userId, u.songId, s.trackId, u.count as songCount, l.wordId, l.pos, l.count as wordCount " +
      "from userTaste as u " +
      "inner join songInfo as s on u.songId=s.songId " +
      "inner join lyricsInfo as l on s.trackId=l.trackId")

    val userSongLyricsPosInfoRdd : RDD[UserSongLyricsPosInfo] = getUserSongLyricsPosInfo(combinedView.rdd)

    val filteredUserSongLyricsPosInfoRdd = getFilteredUserSongLyricsPosInfoRdd(userSongLyricsPosInfoRdd)

    val userVectorMapping = filteredUserSongLyricsPosInfoRdd.map(r => {

      val vector = Vectors.dense(r._2.map(entry => entry.wordId.toDouble).toArray)
      (r._1, vector)
    })

    val lyricsUsersClusters = getOrCreateLyricsUsersKMeansModel(spark, userVectorMapping, LYRICS_USERS_K_MEANS_MODEL_TRAIN_LOCATION)
    val lyricsSongsClusters = getOrCreateSongLyricsKMeansModel(spark, lyricsWithRelevantPosRdd, LYRICS_SONGS_K_MEANS_MODEL_TRAIN_LOCATION)


//    val userPreference1 = List("TRAAAAV128F421A322", "TRAAABD128F429CF47", "TRAAAED128E0783FAB")
//    val userPreference2 = List("TRAAAAV128F421A322")
//
//    val prediction1 = lyricsUsersClusters.predict(getVectorsForUserSession(userPreference1, lyricsWithRelevantPosRdd))
//
//    val prediction2 = lyricsUsersClusters.predict(getVectorsForUserSession(userPreference2, lyricsWithRelevantPosRdd))
//
//    println("prediction1 : " + prediction1)
//
//    println("prediction2 : " + prediction2)

    (lyricsUsersClusters, lyricsSongsClusters)
  }

  def getOrCreateSongLyricsKMeansModel(spark : SparkSession,
                                       lyricsWithRelevantPosRdd : RDD[LyricsInfo],
                                       location : String) : KMeansModel = {

    val loadedModel = Try(KMeansModel.load(spark.sparkContext, location))

    loadedModel match {
      case Success(model) => {
        println("KMeans Lyrics-Songs Model found")
        model
      }
      case Failure(s) => {
        println("KMeans Lyrics-Songs Model Not Found, Training KMeans Lyrics-Songs")
        val songVectors = getVectorsForAllSongLyrics(lyricsWithRelevantPosRdd).cache()
        createSongLyricsKMeansModel(spark, songVectors, location)
      }
    }
  }

  def createSongLyricsKMeansModel(spark : SparkSession, songVectors : RDD[Vector], location : String) : KMeansModel = {

    val songClusters = KMeans.train(songVectors, 28, 30)
    val wssse = songClusters.computeCost(songVectors)
    println("Song Lyrics KMeans model create with 28 clusters and with wssse : " + wssse)
    songClusters.save(spark.sparkContext, location)

    songClusters
  }

  def getFilteredUserSongLyricsPosInfoRdd(userSongLyricsPosInfoRdd : RDD[UserSongLyricsPosInfo]) = userSongLyricsPosInfoRdd
    .groupBy(us => us.userId)
    .flatMap(us1 => us1._2.groupBy(us2 => us2.pos)
      .map(us3 => us3._2.toList.sortWith((a, b) => a.wordCount > b.wordCount).take(3)))
    .flatMap(a => a)
    .groupBy(a => a.userId)
    .filter(p => p._2.size == 9)

  def getOrCreateLyricsUsersKMeansModel(spark : SparkSession,
                                        userVectorMapping : RDD[(String, Vector)], location : String) : KMeansModel = {

        val loadedModel = Try(KMeansModel.load(spark.sparkContext, location))

        loadedModel match {
          case Success(model) => {
            println("KMeans Lyrics-Users Model found")
            model
          }
          case Failure(s) => {
            println("KMeans Lyrics-Users Model Not Found, Training KMeans Lyrics-Users")
            createLyricsUsersKMeansModel(spark, userVectorMapping, location)
          }
        }
  }

  def createLyricsUsersKMeansModel(spark : SparkSession,
                                        userVectorMapping : RDD[(String, Vector)], location : String) : KMeansModel = {


    val parsedData = userVectorMapping.map(entry => entry._2)
    val clusters = KMeans.train(parsedData, 8, 20)

    val wssse = clusters.computeCost(parsedData)

    println("Lyrics Users KMeans Model created with 8 clusters with WSSSE : " + wssse)

    clusters.save(spark.sparkContext, location)

    val vectorPredictionMapping = parsedData.map(vector => {
      val prediction = clusters.predict(vector)
      (vector, prediction)
    })
    clusters

  }

  def getVectorsForAllSongLyrics(lyricsWithRelevantPosRdd : RDD[LyricsInfo]) : RDD[Vector] = {
    lyricsWithRelevantPosRdd.groupBy(song => song.trackId).map(sl => (sl._1,sl._2.groupBy(us2 => us2.pos).map(us3 =>(us3._1,us3._2.toList
      .sortWith((l1, l2) => l1.count > l2.count)
      .take(2))).map(mv => {
      if(mv._2.size < 2) {
        val zerosToBeAdded = 2 - mv._2.size
        val zerosList = List.fill(zerosToBeAdded)(LyricsInfo("",0,0,mv._1))
        val newMv = List.concat(mv._2, zerosList)
        (newMv)
      } else (mv._2)
    })))
      .map(m => Vectors.dense(m._2.flatMap(a=>a).map(e => e.wordId.toDouble).toArray))
      .filter(v => v.size == 6)
  }

  def getUserSongLyricsPosInfo(rdd : RDD[Row]) : RDD[UserSongLyricsPosInfo] = rdd.map(row => UserSongLyricsPosInfo(row.getAs("userId"), row.getAs("songId"), row.getAs("trackId"), row.getAs[Int]("songCount"), row.getAs[Int]("wordId"), row.getAs("pos"), row.getAs[Int]("wordCount")))


}
