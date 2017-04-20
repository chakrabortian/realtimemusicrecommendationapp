package music.recommendation.core

import music.recommendation.bo._

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.Map
import scala.util.{Failure, Success, Try}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

/**
  * Created by deveshkandpal on 4/14/17.
  */
object ExistingUserTest {


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

    println("*** LOADING K-MEANS MODEL ***")
    // Load KMeans cluster
    val clusters = KMeansModel.load(spark.sparkContext, K_MEANS_MODEL_TRAIN_LOCATION)

    // predict for a dummy user
    val predictedCluster = clusters.predict(Vectors.dense(Array[Double](1099.0,253.0,131.0)))

    val artistReversedZip = reverseZip(artistZip)
    val choice = List[String](artistReversedZip(1099), artistReversedZip(253), artistReversedZip(131))
    println("Cold start user choice" + choice)
    println("Cold start user cluster id :  " + predictedCluster)

    println("*** LOADING ALS MODEL ***")
    val predictedALSModel = MatrixFactorizationModel.load(spark.sparkContext, ALS_MODEL_TRAIN_LOCATION + predictedCluster)

    val recos = predictedALSModel.recommendProducts(userZip("9e077c923c655fca51bd803027156c42192283ee").toInt, 10)

    recos.foreach(r => {

      if(!choice.contains(r.product))
      println("recommendations : " + artistReversedZip(r.product.toLong) + " rating : " + r.rating)
      else
        println(" already provided : " + artistReversedZip(r.product.toLong) + " rating : " + r.rating)

    })




  }



  def reverseZip(input : Map[String, Long]) : Map[Long, String] = input.map(i => (i._2, i._1))

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



  def convertToSongInfo(rawRdd : RDD[Row]) : RDD[Option[SongInfo]] = rawRdd.map(row => {

    val x = row.mkString
    parse(x) match {
      case Success(m) => Some(m)
      case Failure(n) => None
    }
  })

  def parse(x : String) : Try[SongInfo] = Try {
    val splitted = x.split("<SEP>")
    SongInfo(splitted(2), splitted(3), splitted(1), splitted(0))
  }

  def convertToUserSongTaste(rawRdd : RDD[Row]) : RDD[UserTaste] = rawRdd.map(row => {
    val x = row.getString(0).split("\\W")
    UserTaste(x(0).trim,x(1).trim,x(2).trim.toInt)
  })

}
