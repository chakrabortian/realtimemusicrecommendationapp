package music.recommendation.core

import music.recommendation.bo.SongInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.{Failure, Success, Try}
import scala.io.Source

/**
  * Created by deveshkandpal on 4/6/17.
  */
object TestLogic {


  val RAW_SONG_INFO_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/" +
    "subset/msd/MillionSongSubset/AdditionalFiles/subset_unique_tracks.txt"
  def main2(args : Array[String]) : Unit = {


    val spark : SparkSession = SparkSession
      .builder().master("local")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "3g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()
    import spark.implicits._

    val rawSongRdd = spark.read.text(RAW_SONG_INFO_LOCATION).rdd

    val artistTrackDf = convertToSongInfo(rawSongRdd).flatMap(f => f.map(q => q)).toDF()

    artistTrackDf.collect()







  }


  def convertToSongInfo(rawRdd : RDD[Row]) : RDD[Option[SongInfo]] = rawRdd.map(row => {

    println("1 ===> "+ row)
    println("2 ====>" + row.mkString)
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
}
