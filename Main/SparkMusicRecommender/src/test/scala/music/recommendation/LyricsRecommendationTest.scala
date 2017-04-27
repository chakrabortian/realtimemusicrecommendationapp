package music.recommendation

import music.recommendation.algorithms.LyricsRecommendation
import music.recommendation.bo.LyricsInfo
import music.recommendation.core.MusicRecommendation
import music.recommendation.utility.SparkUtility
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by Anindita on 4/26/17.
  */
class LyricsRecommendationTest extends FlatSpec with Matchers with BeforeAndAfter {


  before {
    SparkUtility.spark
  }

  val spark = SparkUtility.spark
  import spark.implicits._
  behavior of "Get vectors for song lyrics"
  it should "correctly acquire vectors for song lyrics" in {
    val l1 = LyricsInfo("T1", 5, 10, "Adjective")
    val l2 = LyricsInfo("T1", 89, 9, "Adjective")
    val l3 = LyricsInfo("T1", 23, 12, "Adverb")
    val l4 = LyricsInfo("T1", 45, 21, "Adverb")
    val l5 = LyricsInfo("T1", 101, 25, "Verb")
    val l6 = LyricsInfo("T1", 34, 50, "Verb")

    val finalList = spark.sparkContext.parallelize(List(l1, l2, l3, l4, l5, l6))

    val out = LyricsRecommendation.getVectorsForAllSongLyrics(finalList)

    val collected = out.collect()
    collected(0)._2.size shouldBe(6)
    collected(0)._1 shouldBe("T1")


  }

  behavior of "Check for getUserSongLyricsPosInfo"
  it should "correctly parse into RDD[UserSongLyricsPosInfo]" in {

  val in = spark.sparkContext.parallelize(List(("U1", "S1", "T1", 10, 23,"Verb", 20)))
    .toDF("userId", "songId", "trackId", "songCount", "wordId", "pos", "wordCount").rdd

    val out = LyricsRecommendation.getUserSongLyricsPosInfo(in)

    out.collect()(0).userId shouldBe("U1")



  }


}
