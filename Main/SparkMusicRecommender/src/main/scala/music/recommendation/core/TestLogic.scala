package music.recommendation.core

import music.recommendation.bo._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

import scala.collection.Map
import scala.util.{Failure, Success, Try}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

import scala.io.Source

/**
  * Created by deveshkandpal on 4/6/17.
  */
object TestLogic {

  case class UserArtist(userId : String, artistId : Long)

  val RAW_SONG_INFO_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/" +
    "subset/msd/MillionSongSubset/AdditionalFiles/subset_unique_tracks.txt"


  val RAW_USER_TASTE_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/subset/" +
    "tasteprofile/tasteprofile.txt"


  val ALS_MODEL_TRAIN_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/model/train/"

  val K_MEANS_MODEL_TRAIN_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/model/kmeans"

  def main(args : Array[String]) : Unit = {


    val spark : SparkSession = SparkSession
      .builder().master("local")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "7g")
      .config("spark.executor.memory", "3g")
      .getOrCreate()


    import spark.implicits._


    val rawSongRdd = spark.read.text(RAW_SONG_INFO_LOCATION).rdd

    val rawUserTasteRdd = spark.read.text(RAW_USER_TASTE_LOCATION).rdd

    val artistTrackDf = convertToSongInfo(rawSongRdd).flatMap(f => f.map(q => q)).toDF()

    val userTasteDf = convertToUserSongTaste(rawUserTasteRdd).toDF()


    artistTrackDf.createOrReplaceTempView("songInfo")
    userTasteDf.createOrReplaceTempView("userTaste")

    val combinedDf = spark.sql(
      "select u.userId, s.artist , sum(u.count) as count " +
        "from userTaste as u " +
        "inner join songInfo as s" +
        " on u.songId=s.songId" +
        " group by u.userId, s.artist").cache()



    val combinedRdd : RDD[ArtistCount] = convertCombinedDfToRdd(combinedDf)

    val groupedFeatureVector =  combinedRdd.groupBy(k => k.userId).filter(f => f._2.size >=3 ).map(record => record._2.toList).map(newRec => newRec.sortWith((fe, se) => fe.count > se.count).dropRight(newRec.size - 3))

    val flattennedFeatureVector = flattenRDD(groupedFeatureVector)

    val userZip : Map[String, Long] = prepareUserZip(combinedRdd)
    val artistZip : Map[String, Long] = prepareArtistZip(combinedRdd)


    val artistGroupedByUser = convertArtistNameToIndex(flattennedFeatureVector.toDF(), artistZip).groupBy(u => u.userId)

    val userVectorMapping = artistGroupedByUser.map(entry => {
      val vector = Vectors.dense(entry._2.map(ua => ua.artistId.toDouble).toArray)
      (entry._1, vector)
    }).cache()


    val parsedData = userVectorMapping.map(entry => entry._2)


    val numClusters = 12
    val numIterations = 30
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    clusters.save(spark.sparkContext, K_MEANS_MODEL_TRAIN_LOCATION)

    val vectorPredictionMapping = parsedData.map(vector => {
      val prediction = clusters.predict(vector)
      (vector, prediction)
    })


    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)


//    val userVectorMappingDf = userVectorMapping.toDF("userId", "vector")
//    val vectorPredictionMappingDf = vectorPredictionMapping.toDF("vector", "prediction")
//
//     val joinedUserPredictionMappingDf = userVectorMappingDf.join(vectorPredictionMappingDf, "vector")
//
//    val userPredictionRDD  = joinedUserPredictionMappingDf.rdd.map(row => UserPrediction(row.getAs("userId"), row.getAs("prediction")))
//
//    val ratings = convertCombinedRddToRating(combinedRdd, userZip, artistZip)
//
//    val reversedUserZip = reverseUserZip(userZip)
//
//    val userIdPredictionRdd = userPredictionRDD.map(upRdd => (userZip(upRdd.userId).toInt, upRdd.prediction))
//
//      val userIdPredictionDf = userIdPredictionRdd.toDF("user", "clusterId")
//
//    val alsKMeansDf = userIdPredictionDf.join(ratings.toDF(), "user")
//
//    alsKMeansDf.show()
//
//    alsKMeansDf.printSchema()
//
//
//    val alsRatingRDD = alsKMeansDf.rdd.map(alsr => (Integer.valueOf(alsr.getAs("clusterId").toString), Rating(alsr.getAs("user"), alsr.getAs("product"), alsr.getAs("rating"))))
//
//
//    val collected = alsRatingRDD.groupByKey().collectAsMap()
//
//    for((k, v) <- collected) {
//      val clusterId = k
//      val filteredRating = spark.sparkContext.parallelize(v.toList)
//      val model = ALS.trainImplicit(filteredRating, 10, 5, 0.01, 1.0)
//      model.save(spark.sparkContext,  ALS_MODEL_TRAIN_LOCATION + clusterId)
//      println("Model Saved at : " + ALS_MODEL_TRAIN_LOCATION + clusterId)
//
//    }

  }

  def reverseUserZip(input : Map[String, Long]) : Map[Long, String] = input.map(i => (i._2, i._1))

  def flattenRDD(input : RDD[List[ArtistCount]]) : RDD[ArtistCount] = input.flatMap(a => a.map(b => b))

  def convertArtistNameToIndex(df :DataFrame, artistZip : Map[String, Long]) : RDD[UserArtist] = df.rdd.map(row => UserArtist(row.getAs("userId"), artistZip(row.getAs("artist"))))

  def convertCombinedDfToRdd(df : DataFrame) : RDD[ArtistCount] = df.rdd.map(row => ArtistCount(row.getAs("userId"), row.getAs("artist"), row.getAs("count")))


  def prepareUserZip(data : RDD[ArtistCount])
  : Map[String, Long] = data
    .map(a => a.userId).distinct()
    .sortBy(x => x).zipWithIndex.collectAsMap()

  def prepareArtistZip(data: RDD[ArtistCount])
  : Map[String, Long] = data
    .map(a => a.artist).distinct()
    .sortBy(x => x).zipWithIndex.collectAsMap()

  def convertCombinedRddToRating(data : RDD[ArtistCount],
                                 users : Map[String, Long], artists : Map[String, Long]) : RDD[Rating] =

    data.map(r => Rating(users(r.userId).toInt, artists(r.artist).toInt, r.count))


  def extractALSFeatures() : RDD[ArtistCount] = ???

  def convertToSongInfo2(rawRdd : RDD[Row]) : RDD[SongInfo] = rawRdd.map( row => SongInfo(row.getAs("artist"),
    row.getAs("title"), row.getAs("track_id")))

  def convertToSongInfo(rawRdd : RDD[Row]) : RDD[Option[SongInfo]] = rawRdd.map(row => {

    val x = row.mkString
    parse(x) match {
      case Success(m) => Some(m)
      case Failure(n) => None
    }
  })

  def parse(x : String) : Try[SongInfo] = Try {
    val splitted = x.split("<SEP>")
    SongInfo(splitted(2), splitted(3), splitted(1))
  }

  def convertToUserSongTaste(rawRdd : RDD[Row]) : RDD[UserTaste] = rawRdd.map(row => {
    val x = row.getString(0).split("\\W")
    UserTaste(x(0).trim,x(1).trim,x(2).trim.toInt)
  })


}
