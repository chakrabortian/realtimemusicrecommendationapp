package music.recommendation.core




import music.recommendation.bo.{ArtistCount, SongInfo, UserTaste}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.Map
import scala.util.{Failure, Success, Try}


/**
  * Created by chakr on 4/8/17.
  */



object SparkMain {

  val RAW_USER_TASTE_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/subset/" +
    "tasteprofile/tasteprofile.txt"

  val RAW_SONG_INFO_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/" +
    "subset/msd/MillionSongSubset/AdditionalFiles/subset_unique_tracks.txt"

  val ALS_MODEL_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/model/model.obj"

  val ALS_MODEL_TRAIN_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/model/train"

  val ALS_MODEL_CV_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/model/cv"

  def main(args : Array[String]) : Unit = {


    val spark : SparkSession = SparkSession
      .builder().master("local")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "5g")
      .config("spark.executor.memory", "5g")
      .getOrCreate()


    import spark.implicits._

    val rawSongRdd = spark.read.text(RAW_SONG_INFO_LOCATION).rdd

    val rawUserTasteRdd = spark.read.text(RAW_USER_TASTE_LOCATION).rdd

    val artistTrackDf = convertToSongInfo(rawSongRdd).flatMap(f => f.map(q => q)).toDF()

    val userTasteDf = convertToUserSongTaste(rawUserTasteRdd).toDF()

    artistTrackDf.createOrReplaceTempView("songInfo")
    userTasteDf.createOrReplaceTempView("userTaste")

    val combinedDf = spark.sql(
      "select u.userId, s.artist, sum(u.count) as count " +
      "from userTaste as u " +
      "inner join songInfo as s" +
      " on u.songId=s.songId" +
      " group by u.userId, s.artist").cache()


    val combinedRdd = convertCombinedDfToRdd(combinedDf)
    val userZip : Map[String, Long] = prepareUserZip(combinedRdd)
    val artistZip : Map[String, Long] = prepareArtistZip(combinedRdd)
    // val ratings = convertCombinedRddToRating(combinedRdd, userZip, artistZip)

    // val model = ALS.trainImplicit(ratings, 10, 5, 0.01, 1.0)



     val testUserName  = userZip.keys.toList(0)
    // val testUserZipIndex = userZip(testUserName)


    val testUserArtistDf = spark.sql(s"""Select s.artist from songInfo as s inner join userTaste as u on u.songId = s.songId where u.userId='$testUserName' """)


    testUserArtistDf.show()


    // model.save(spark.sparkContext, ALS_MODEL_LOCATION)


     val loadedModel = MatrixFactorizationModel.load(spark.sparkContext, ALS_MODEL_LOCATION)

      // loadedModel.recommendProducts()

    val newUser = "cc6fa9c4708267c41611662eb28905664453b91d";

    val newUserZipId = userZip(newUser)

    val recommendations = loadedModel.recommendProducts(333772, 5)

    val inverseArtistZip = artistZip.map(a => a._2 -> a._1)

    recommendations.foreach(r => println(inverseArtistZip(r.product)))





    //println(p)

  }




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
