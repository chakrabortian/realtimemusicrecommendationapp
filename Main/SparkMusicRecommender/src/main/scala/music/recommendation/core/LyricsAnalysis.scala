package music.recommendation.core


import music.recommendation.bo._
import org.apache.spark.sql.functions._
import music.recommendation.utility.NLPUtility
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
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

  val K_MEANS_SONGS_MODEL_TRAIN_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/model/songs/kmeans"

  def main(args : Array[String]): Unit = {

    val spark : SparkSession = SparkSession
      .builder().master("local")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "5g")
      .config("spark.executor.memory", "5g")
      .getOrCreate()


    import spark.sqlContext.implicits._



    val rawUserTasteRdd = spark.read.text(RAW_USER_TASTE_LOCATION).rdd
    val rawSongRdd = spark.read.text(RAW_SONG_INFO_LOCATION).rdd
    val rawWordDict = spark.read.text(RAW_WORD_DICTIONARY_LOCATION).rdd
     val rawTrackLyrics = spark.read.text(RAW_TRACK_LYRICS_LOCATION).rdd

    val userTasteRdd : RDD[UserTaste] = convertToUserSongTaste(rawUserTasteRdd).groupBy(a => a.userId).filter(records => records._2.size >=3).flatMap(a => a._2)



    val artistTrackRdd : RDD[SongInfo] = convertToSongInfo(rawSongRdd).flatMap(f => f.map(q => q))



    val tempWordDictionary = convertToDict(rawWordDict).toDF().select('_1.as("wordId"), NLPUtility.getPartOfSpeech('_2).as("pos"))

    val wordPosDictionary = tempWordDictionary.rdd.map(row => (row.getAs[Int]("wordId"), row.getAs[Seq[String]]("pos")(0))).collectAsMap()






    spark.sparkContext.broadcast(wordPosDictionary)

    val lyricsInfoRdd : RDD[LyricsInfo] = convertToLyricsInfo(rawTrackLyrics, wordPosDictionary)


    val lyricsWithRelevantPosRdd : RDD [LyricsInfo] = getLyricsWithRelevantPos(lyricsInfoRdd).cache()

 //   val lyricsWithRelevantPosDf = lyricsWithRelevantPosRdd.toDF()
//    val userTasteDf = userTasteRdd.toDF()
//    val artistTrackDf = artistTrackRdd.toDF()
//
//    lyricsWithRelevantPosDf.createOrReplaceTempView("lyricsInfo")
//    userTasteDf.createOrReplaceTempView("userTaste")
//    artistTrackDf.createOrReplaceTempView("songInfo")
//
//    val combinedView = spark.sql("select u.userId, u.songId, s.trackId, u.count as songCount, l.wordId, l.pos, l.count as wordCount " +
//      "from userTaste as u " +
//      "inner join songInfo as s on u.songId=s.songId " +
//      "inner join lyricsInfo as l on s.trackId=l.trackId")
//
//    val userSongLyricsPosInfoRdd : RDD[UserSongLyricsPosInfo] = getUserSongLyricsPosInfo(combinedView.rdd)
//
//    val result = userSongLyricsPosInfoRdd
//      .groupBy(us => us.userId)
//      .flatMap(us1 => us1._2.groupBy(us2 => us2.pos)
//        .map(us3 => us3._2.toList.sortWith((a, b) => a.wordCount > b.wordCount).take(3)))
//      .flatMap(a => a)
//      .groupBy(a => a.userId)
//      .filter(p => p._2.size == 9)
//
//    val userVectorMapping = result.map(r => {
//
//      val vector = Vectors.dense(r._2.map(entry => entry.wordId.toDouble).toArray)
//      (r._1, vector)
//    })




//    val parsedData = userVectorMapping.map(entry => entry._2)
//
//      val clusters = KMeans.train(parsedData, 8, 20)
//
//      val wssse = clusters.computeCost(parsedData)
//
//      clusters.save(spark.sparkContext, K_MEANS_LYRICS_MODEL_TRAIN_LOCATION)
//
//      val vectorPredictionMapping = parsedData.map(vector => {
//            val prediction = clusters.predict(vector)
//            (vector, prediction)
//          })

   // val clusters = KMeansModel.load(spark.sparkContext, K_MEANS_LYRICS_MODEL_TRAIN_LOCATION)





//    val userPreference1 = List("TRAAAAV128F421A322", "TRAAABD128F429CF47", "TRAAAED128E0783FAB")
//    val userPreference2 = List("TRAAAAV128F421A322")
//
//    val prediction1 = clusters.predict(getVectorsForUserSession(userPreference1, lyricsWithRelevantPosRdd))
//
//    val prediction2 = clusters.predict(getVectorsForUserSession(userPreference2, lyricsWithRelevantPosRdd))
//
//    println("prediction1 : " + prediction1)
//
//    println("prediction2 : " + prediction2)



 //   val songVectors = getVectorsForAllSongLyrics(lyricsWithRelevantPosRdd).cache()
    // val l = List(8,12,14,16,18,20, 22, 24, 26,28,30)

    //for(x <- l) {
//    val x = 28
//      val songClusters = KMeans.train(songVectors, x, 30)
//      val wssse = songClusters.computeCost(songVectors)
//      println("Song vectors wssse : " + wssse + " cluster size : " +x)
    //}

    //songClusters.save(spark.sparkContext, K_MEANS_SONGS_MODEL_TRAIN_LOCATION)

    println("vector size : " + lyricsInfoRdd.groupBy(k => k.trackId).collectAsMap().size)


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

  def getVectorsForUserSession(trackIds : List[String], lyricsWithRelevantPosRdd : RDD[LyricsInfo]) : RDD[Vector] = {

    val filteredLyricsInfo = lyricsWithRelevantPosRdd
      .filter( rdd => trackIds.contains(rdd.trackId))
      .groupBy(us2 => us2.pos).map(us3 => us3._2.toList
      .sortWith((l1, l2) => l1.count > l2.count)
      .take(3))
      .flatMap(fm => fm)
      .groupBy(gb => gb.pos).map(mv => {
      if(mv._2.size < 3) {
        val zerosToBeAdded = 3 - mv._2.size
        val zerosList = List.fill(zerosToBeAdded)(LyricsInfo("",0,0,mv._1))
        val newMv = List.concat(mv._2, zerosList)
        (mv._1, newMv)
      } else (mv._1, mv._2)
    })

    filteredLyricsInfo.map(fli => Vectors.dense(fli._2.map(f => f.wordId.toDouble).toArray))

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
