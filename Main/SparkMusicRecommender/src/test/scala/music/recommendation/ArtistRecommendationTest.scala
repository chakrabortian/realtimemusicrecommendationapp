package music.recommendation

import music.recommendation.algorithms.{ArtistRecommendation, GenerateRecommendations}
import music.recommendation.bo.ArtistCount
import music.recommendation.core.MusicRecommendation
import music.recommendation.utility.SparkUtility
import org.apache.spark.mllib.recommendation.Rating
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by Priyesh on 4/19/17.
  */
class ArtistRecommendationTest extends FlatSpec with Matchers with BeforeAndAfter {

  before {
    SparkUtility.spark
  }
  val spark = SparkUtility.spark
  import spark.implicits._



  behavior of "Test for Getting Grouped Feature Vector"
  it should "correctly parse into user map" in {
    val in = spark.sparkContext.parallelize(List(("U1", "A1", 10L), ("U1", "A2", 10L), ("U1", "A3", 10L), ("U1", "A4", 10L))).toDF("userId", "artist", "count")
    val rdd = MusicRecommendation.convertCombinedDfToRdd(in)
    val out = ArtistRecommendation.getGroupedFeatureVector(rdd)
    out.collect()(0).size shouldBe(3)

  }

  behavior of "Test For RDD flattenning"
  it should "correctly convert RDD[List[]] to RDD[]" in {


    val in = spark.sparkContext.parallelize(List(("U1", "A1", 10L), ("U1", "A2", 10L), ("U1", "A3", 10L), ("U1", "A4", 10L))).toDF("userId", "artist", "count")
    val rdd = MusicRecommendation.convertCombinedDfToRdd(in)
    val out = ArtistRecommendation.getGroupedFeatureVector(rdd)
    val finalOut = ArtistRecommendation.flattenRDD(out)
    finalOut.collect().length shouldBe(3)

  }

  behavior of "Test For convertArtistNameToIndex"
  it should "should return an RDD[UserArtist]" in {


    val in = spark.sparkContext.parallelize(List(("U1", "A1", 10L), ("U1", "A2", 10L), ("U1", "A3", 10L), ("U1", "A4", 10L))).toDF("userId", "artist", "count")
    val rdd = MusicRecommendation.convertCombinedDfToRdd(in)
    val out = ArtistRecommendation.getGroupedFeatureVector(rdd)
    val tempOut = ArtistRecommendation.flattenRDD(out).toDF()
    val az = scala.collection.immutable.Map[String, Long]("A1" -> 0L , "A2" -> 1, "A3" -> 2, "A4" -> 3)
    val finalOut = ArtistRecommendation.convertArtistNameToIndex(tempOut, az)

    finalOut.collect().length shouldBe(3)

  }

  behavior of "Test For getUserVectorMapping"
  it should "should return an should return an rdd of userId and corresponding vector" in {


    val in = spark.sparkContext.parallelize(List(("U1", "A1", 10L), ("U1", "A2", 10L), ("U1", "A3", 10L), ("U1", "A4", 10L))).toDF("userId", "artist", "count")
    val rdd = MusicRecommendation.convertCombinedDfToRdd(in)
    val out = ArtistRecommendation.getGroupedFeatureVector(rdd)
    val tempOut = ArtistRecommendation.flattenRDD(out).toDF()
    val az = scala.collection.immutable.Map[String, Long]("A1" -> 0L , "A2" -> 1, "A3" -> 2, "A4" -> 3)
    val indexedArtistName = ArtistRecommendation.convertArtistNameToIndex(tempOut, az)
    val finalOut = ArtistRecommendation.getUserVectorMapping(indexedArtistName.groupBy(g => g.userId))

    val collected = finalOut.collect()
    collected(0)._1 shouldBe("U1")
    collected(0)._2.size shouldBe(3)

  }

  behavior of "Test For checking Rating object"
  it should "should return correct rating object for the user" in {

    val ac = ArtistCount("U1", "A1", 10L)
    val users = scala.collection.immutable.Map[String, Long]("U1" -> 0L)
    val artists = scala.collection.immutable.Map[String, Long]("A1" -> 0L)
    val input = spark.sparkContext.parallelize((List(ac)))
    val output = ArtistRecommendation.convertCombinedRddToRating(input, users, artists)
    output.collect()(0) match {
      case Rating(x,y, z) => z shouldBe(10)
      case _ =>
    }

  }





  }
