package music.recommendation.core


import music.recommendation.bo._
import org.apache.spark.sql.functions._
import music.recommendation.utility.NLPUtility
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.{Map, mutable}
import scala.util.{Failure, Success, Try}



/**
  * Created by priyeshkaushik on 4/15/17.
  *
  */
object LyricsAnalysis {



  val RAW_SONG_INFO_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/" +
    "subset/msd/MillionSongSubset/AdditionalFiles/subset_unique_tracks.txt"


  val RAW_USER_TASTE_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/subset/" +
    "tasteprofile/tasteprofile.txt"


  val RAW_WORD_DICTIONARY_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/subset/musicxmatch/wordDictionary.txt"

  val RAW_TRACK_LYRICS_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/subset/musicxmatch/lyrics.txt"

  val K_MEANS_LYRICS_MODEL_TRAIN_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/model/lyrics/kmeans"

  def main(args : Array[String]): Unit = {

    val spark : SparkSession = SparkSession
      .builder().master("local")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "7g")
      .config("spark.executor.memory", "3g")
      .getOrCreate()


    import spark.sqlContext.implicits._



    val rawUserTasteRdd = spark.read.text(RAW_USER_TASTE_LOCATION).rdd
    val rawSongRdd = spark.read.text(RAW_SONG_INFO_LOCATION).rdd
    val rawWordDict = spark.read.text(RAW_WORD_DICTIONARY_LOCATION).rdd
     val rawTrackLyrics = spark.read.text(RAW_TRACK_LYRICS_LOCATION).rdd

    val userTasteRdd : RDD[UserTaste] = convertToUserSongTaste(rawUserTasteRdd).groupBy(a => a.userId).filter(records => records._2.size >=3).flatMap(a => a._2)



    val artistTrackRdd : RDD[SongInfo] = convertToSongInfo(rawSongRdd).flatMap(f => f.map(q => q))

    //val wordDictionary = convertToDict(rawWordDict).collectAsMap()

    val tempWordDictionary = convertToDict(rawWordDict).toDF().select('_1.as("wordId"), NLPUtility.pos('_2).as("pos"))

    val wordPosDictionary = tempWordDictionary.rdd.map(row => (row.getAs[Int]("wordId"), row.getAs[Seq[String]]("pos")(0))).collectAsMap()






    spark.sparkContext.broadcast(wordPosDictionary)

    val lyricsInfoRdd : RDD[LyricsInfo] = convertToLyricsInfo(rawTrackLyrics, wordPosDictionary)

   // val lyricsInfoDf  = lyricsInfoRdd.toDF("trackId", "word", "count", "pos")

   // val lyricsWithPosInfoDf = lyricsInfoDf.select('trackId, 'word, 'count, NLPUtility.pos('word).as("pos"))

//    val lyricsWithPosInfoRdd = parseLyricsInfoWithPos(lyricsWithPosInfoDf.rdd)

    val lyricsWithRelevantPosRdd : RDD [LyricsInfo] = getLyricsWithRelevantPos(lyricsInfoRdd)

    val lyricsWithRelevantPosDf = lyricsWithRelevantPosRdd.toDF()
    val userTasteDf = userTasteRdd.toDF()
    val artistTrackDf = artistTrackRdd.toDF()

    lyricsWithRelevantPosDf.createOrReplaceTempView("lyricsInfo")
    userTasteDf.createOrReplaceTempView("userTaste")
    artistTrackDf.createOrReplaceTempView("songInfo")

    val combinedView = spark.sql("select u.userId, u.songId, s.trackId, u.count as songCount, l.wordId, l.pos, l.count as wordCount " +
      "from userTaste as u " +
      "inner join songInfo as s on u.songId=s.songId " +
      "inner join lyricsInfo as l on s.trackId=l.trackId")

    val userSongLyricsPosInfoRdd : RDD[UserSongLyricsPosInfo] = getUserSongLyricsPosInfo(combinedView.rdd)

    //val result = userSongLyricsPosInfoRdd.groupBy(u => u.userId).map(user => user._2.toList.groupBy(song => song.songId).map(word => word._2.groupBy(w => w.pos).map(k => k._2.sortWith((p1, p2) => p1.wordCount > p2.wordCount))))



//    val result = userSongLyricsPosInfoRdd.groupBy(u => u.userId).flatMap(songs => songs._2.toList.groupBy(speech => speech.pos)
//      .flatMap(pos => pos._2.sortWith((p1, p2) => p1.wordCount > p2.wordCount).take(20))).toDF().show(1000)


    val result = userSongLyricsPosInfoRdd
      .groupBy(us => us.userId)
      .flatMap(us1 => us1._2.groupBy(us2 => us2.pos)
        .map(us3 => us3._2.toList.sortWith((a, b) => a.wordCount > b.wordCount).take(3)))
      .flatMap(a => a)
      .groupBy(a => a.userId)
      .filter(p => p._2.size == 9)




//    val userVectorMapping = artistGroupedByUser.map(entry => {
//      val vector = Vectors.dense(entry._2.map(ua => ua.artistId.toDouble).toArray)
//      (entry._1, vector)
//    }).cache()

    val userVectorMapping = result.map(r => {

      val vector = Vectors.dense(r._2.map(entry => entry.wordId.toDouble).toArray)
      (r._1, vector)
    })

    val parsedData = userVectorMapping.map(entry => entry._2)



    val numClusters = (2 to 16 by 2).toList

    val numIterations = (10 to 30 by 5).toList

    var WSSSE = 0.0
    var clusterNoMin = 0
    var minIter = 0
    for(x <- numClusters; iter <- numIterations) {
      val clusters = KMeans.train(parsedData, x, iter)

      val newWSSSE = clusters.computeCost(parsedData)

      println("Within Set Sum of Squared Errors = " + newWSSSE + " for cluster : " + x + " iter : " + iter)
      if(WSSSE == 0.0) {
        WSSSE = newWSSSE
        clusterNoMin = x
        minIter = iter

      }
      else {
        if (newWSSSE < WSSSE) {
          WSSSE = newWSSSE
          clusterNoMin = x
          minIter = iter
        }
      }
    }

    println("Final selected cluster is : " + clusterNoMin + " and iteration is : " + minIter)

    val clusters = KMeans.train(parsedData, clusterNoMin, minIter)

    clusters.save(spark.sparkContext, K_MEANS_LYRICS_MODEL_TRAIN_LOCATION)


//          val vectorPredictionMapping = parsedData.map(vector => {
//            val prediction = clusters.predict(vector)
//            (vector, prediction)
//          })













//    result.createOrReplaceTempView("result")

    //spark.sql("Select r.userId, r.pos, r.wordId from result as r where pos in (select pos from result where pos = 'JJ' limit 2) group by r.userId,r.pos,r.wordId ").show(100)

    // spark.sql("Select r.userId, (select pos from result where pos = 'JJ' limit 2) as adj , r.wordId from result as r group by r.userId,r.pos,r.wordId ").show(100)








   // val artistTrackDf = convertToSongInfo(rawSongRdd).flatMap(f => f.map(q => q)).toDF()


   // val str1 = Seq("It was an excellent movie", "good was it").toDF("words")

    // str1.select(NLPUtility.sentiment('words).as("pos")).show()





  }

  def getUserSongLyricsPosInfo(rdd : RDD[Row]) : RDD[UserSongLyricsPosInfo] = rdd.map(row => UserSongLyricsPosInfo(row.getAs("userId"), row.getAs("songId"), row.getAs("trackId"), row.getAs[Int]("songCount"), row.getAs[Int]("wordId"), row.getAs("pos"), row.getAs[Int]("wordCount")))

  def getLyricsWithRelevantPos(rdd : RDD[LyricsInfo]) : RDD[LyricsInfo] =  rdd.filter(row => relevantPos.contains(row.pos)).map(row => {
    row match {
      case LyricsInfo(a,b,c,"JJ") => LyricsInfo(a,b,c, "Adjective")
      case LyricsInfo(a,b,c,"JJR") => LyricsInfo(a,b,c, "Adjective")
      case LyricsInfo(a,b,c,"JJS") => LyricsInfo(a,b,c, "Adjective")
      case LyricsInfo(a,b,c,"RB") => LyricsInfo(a,b,c, "Adverb")
      case LyricsInfo(a,b,c,"RBR") => LyricsInfo(a,b,c, "Adverb")
      case LyricsInfo(a,b,c,"RBS") => LyricsInfo(a,b,c, "Adverb")
      case LyricsInfo(a,b,c,"VB") => LyricsInfo(a,b,c, "Verb")
      case LyricsInfo(a,b,c,"WRB") => LyricsInfo(a,b,c, "Adjective")
    }
  })

  val relevantPos = List("JJ", "JJR", "JJS", "RB", "RBR", "RBS", "VB", "WRB")

  def convertToUserSongTaste(rawRdd: RDD[Row]): RDD[UserTaste] = rawRdd.map(row => {
    val x = row.getString(0).split("\\W")
    UserTaste(x(0).trim, x(1).trim, x(2).trim.toInt)
  })

  //def parseLyricsInfoWithPos(rdd : RDD[Row]) : RDD[LyricsInfo] = rdd.map(song => LyricsInfo(song.getAs("trackId"), song.getAs("word"), song.getAs("count").toString.toInt, Option(song.getAs[Seq[String]]("pos").mkString)))

  def convertToLyricsInfo(rawRdd : RDD[Row], dict : Map[Int, String]) : RDD[LyricsInfo]  = rawRdd.flatMap(row => {
    val x = row.mkString
    val lineArr = x.split(",")
    val trackId = lineArr(0)
    val wordArr = lineArr.slice(2, lineArr.length)
    val lyricsInfo = wordArr.map(el => {
      val wordCountArr = el.split(":")
      LyricsInfo(trackId, wordCountArr(0).toInt, wordCountArr(1).toInt, dict(wordCountArr(0).toInt))
    })
    lyricsInfo
  })

//  def parseLyricsInfo(trackId: String, wordArr: String) = Try {
//    val wordCountArr = wordArr.split(":")
//    LyricsInfo(trackId, wordCountArr(0), wordCountArr(1).toInt)
//  }

  def convertToDict(rawRdd : RDD[Row]) : RDD[(Int, String)] = rawRdd.flatMap(row => {
    val x = row.mkString
    x.split(",").toList.zip (Stream from 1).map(a => (a._2, a._1))
  })
  def convertToSongInfo(rawRdd: RDD[Row]): RDD[Option[SongInfo]] = rawRdd.map(row => {

    val x = row.mkString
    parseSong(x) match {
      case Success(m) => Some(m)
      case Failure(n) => None
    }
  })

  def parseSong(x: String): Try[SongInfo] = Try {
    val splitted = x.split("<SEP>")
    SongInfo(splitted(2), splitted(3), splitted(1), splitted(0))
  }

}
