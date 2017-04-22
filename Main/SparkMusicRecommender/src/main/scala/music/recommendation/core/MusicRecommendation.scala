package music.recommendation.core

import music.recommendation.algorithms.{ArtistRecommendation, GenerateRecommendations, LyricsRecommendation}
import music.recommendation.bo._
import music.recommendation.dao.MongoOperations
import music.recommendation.ingestion.DataInjestion
import music.recommendation.utility.FileLocations._
import music.recommendation.utility.{NLPUtility, SparkUtility}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.Map
import scala.util.{Failure, Success, Try}

/**
  * Created by deveshkandpal on 4/18/17.
  */

object MusicRecommendation {

  def main(args : Array[String]) : Unit = {

    val spark = SparkUtility.spark

    import spark.implicits._
    val mongoOps = new MongoOperations()
    val rawSongRdd : RDD[Row] = DataInjestion.readData(spark, RAW_SONG_INFO_LOCATION)
    val rawUserTasteRdd : RDD[Row] = DataInjestion.readData(spark, RAW_USER_TASTE_LOCATION)
    val rawWordDict : RDD[Row]= DataInjestion.readData(spark, RAW_WORD_DICTIONARY_LOCATION)
    val rawTrackLyrics : RDD[Row] = DataInjestion.readData(spark,RAW_TRACK_LYRICS_LOCATION)

    val artistTrackRDD :RDD[SongInfo] = convertToSongInfo(rawSongRdd).flatMap(f => f.map(q => q))
    mongoOps.saveArtistSongInfo(artistTrackRDD)
    val userTasteRDD = convertToUserSongTaste(rawUserTasteRdd)
    val tempWordDictionary = convertToDict(rawWordDict).toDF()
      .select('_1.as("wordId"), NLPUtility.getPartOfSpeech('_2).as("pos"))
    val wordPosDictionary = getWordPosDictionary(tempWordDictionary)
    spark.sparkContext.broadcast(wordPosDictionary)

    val artistTrackDf = artistTrackRDD.toDF()
    val userTasteDf = userTasteRDD.toDF()

    val lyricsInfoRdd : RDD[LyricsInfo] = convertToLyricsInfo(rawTrackLyrics, wordPosDictionary)
    val lyricsWithRelevantPosRdd : RDD [LyricsInfo] = getLyricsWithRelevantPos(lyricsInfoRdd).cache()

    artistTrackDf.createOrReplaceTempView("songInfo")
    userTasteDf.createOrReplaceTempView("userTaste")

    val combinedDf = spark.sql(
      "select u.userId, s.artist , sum(u.count) as count " +
        "from userTaste as u " +
        "inner join songInfo as s" +
        " on u.songId=s.songId" +
        " group by u.userId, s.artist").cache()

    val combinedRdd: RDD[ArtistCount] = convertCombinedDfToRdd(combinedDf)

    mongoOps.saveUserArtistInfo(combinedRdd)
    val userZip: Map[String, Long] = prepareUserZip(combinedRdd)
    val artistZip: Map[String, Long] = prepareArtistZip(combinedRdd)

    val artistRecommendation = new ArtistRecommendation()
    val artistKMeansModel = artistRecommendation.prepareArtistRecommendation(spark, combinedRdd, userZip, artistZip)
    val lyricsRecommendation = new LyricsRecommendation()
    val (lyricsUsersClusters, lyricsSongsTuple) = lyricsRecommendation.prepareLyricsRecommendationModels(spark, lyricsWithRelevantPosRdd,
        userTasteDf, artistTrackDf)
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint("checkpoint")
    val topics = Array("ColdStartRecoRequest","ArtistforArtistRecoRequest","ExistingUserRecoRequest","SongRecoRequest")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, SparkUtility.kafkaParams)
    )


    stream.foreachRDD(rdd => {
      println("********** MESSAGE RECEIVED *********")

      rdd.foreach(c => {
        handleKafkaRecords(new GenerateRecommendations, c,artistKMeansModel, artistZip, userZip,lyricsUsersClusters,lyricsSongsTuple,lyricsWithRelevantPosRdd)
      })

    })



    ssc.start()
    ssc.awaitTermination()
    ssc.stop(false)

  }

  def prepareUserZip(data: RDD[ArtistCount])
  : Map[String, Long] = data
    .map(a => a.userId).distinct()
    .sortBy(x => x).zipWithIndex.collectAsMap()

  def prepareArtistZip(data: RDD[ArtistCount])
  : Map[String, Long] = data
    .map(a => a.artist).distinct()
    .sortBy(x => x).zipWithIndex.collectAsMap()

  def convertCombinedDfToRdd(df: DataFrame): RDD[ArtistCount] = df.rdd
    .map(row => ArtistCount(row.getAs("userId"), row.getAs("artist"), row.getAs("count")))

  def reverseZip(input: Map[String, Long]): Map[Long, String] = input.map(i => (i._2, i._1))

  def handleKafkaRecords(gr : GenerateRecommendations, record : ConsumerRecord[String, String], clusters : KMeansModel, artistZip : Map[String , Long], userZip : Map[String , Long],lyricsUsersClusters : KMeansModel, lyricsSongsTuple : (KMeansModel,RDD[SongVectorPrediction]), lyricsWithRelevantPosRdd : RDD[LyricsInfo]): Any = {
    record.topic() match {
      case "ColdStartRecoRequest" => gr.handleColdStartRecommendation(record,clusters, artistZip)
      case "ExistingUserRecoRequest"=> {
        println("RECEIVED EXISTING USER RECO REQUEST")
        gr.handleExistingUserRecomendation(record,artistZip, userZip)
      }
        case "ArtistforArtistRecoRequest" => {
          println("RECEIVED ARTIST FOR ARTIST RECO REQUEST")
          gr.handleArtistForArtistRecomendation(record,clusters, artistZip)
        }
      case "SongRecoRequest" => {
        println("RECEIVED LYRICS BASED SONG REQUEST")
        gr.handleSongRecomendation(record,lyricsUsersClusters,lyricsSongsTuple,lyricsWithRelevantPosRdd)
      }
      case _ => println("unrecognised topic : ")
    }
  }


  def getLyricsWithRelevantPos(rdd : RDD[LyricsInfo]) : RDD[LyricsInfo] =  rdd.filter(row => NLPUtility.relevantPos.contains(row.pos)).map {
    case LyricsInfo(a, b, c, "JJ") => LyricsInfo(a, b, c, "Adjective")
    case LyricsInfo(a, b, c, "JJR") => LyricsInfo(a, b, c, "Adjective")
    case LyricsInfo(a, b, c, "JJS") => LyricsInfo(a, b, c, "Adjective")
    case LyricsInfo(a, b, c, "RB") => LyricsInfo(a, b, c, "Adverb")
    case LyricsInfo(a, b, c, "RBR") => LyricsInfo(a, b, c, "Adverb")
    case LyricsInfo(a, b, c, "RBS") => LyricsInfo(a, b, c, "Adverb")
    case LyricsInfo(a, b, c, "VB") => LyricsInfo(a, b, c, "Verb")
    case LyricsInfo(a, b, c, "WRB") => LyricsInfo(a, b, c, "Adjective")
  }


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

  def getWordPosDictionary(tempWordDictionary: DataFrame): Map[Int, String] = tempWordDictionary.rdd.map(row => (row.getAs[Int]("wordId"), row.getAs[Seq[String]]("pos")(0))).collectAsMap()

  def convertToDict(rawRdd : RDD[Row]) : RDD[(Int, String)] = rawRdd.flatMap(row => {
    val x = row.mkString
    x.split(",").toList.zip (Stream from 1).map(a => (a._2, a._1))
  })

  def loadKMeansModel(spark : SparkSession, location : String) = Try(KMeansModel.load(spark.sparkContext, location))


  def convertToSongInfo(rawRdd: RDD[Row]): RDD[Option[SongInfo]] = rawRdd.map(row => {

    val x = row.mkString
    parse(x) match {
      case Success(m) => Some(m)
      case Failure(_) => None
    }
  })

  def parse(x: String): Try[SongInfo] = Try {
    val splitted = x.split("<SEP>")
    SongInfo(splitted(2), splitted(3), splitted(1), splitted(0))
  }

  def convertToUserSongTaste(rawRdd: RDD[Row]): RDD[UserTaste] = rawRdd.map(row => {
    val x = row.getString(0).split("\\W")
    UserTaste(x(0).trim, x(1).trim, x(2).trim.toInt)
  })





}
