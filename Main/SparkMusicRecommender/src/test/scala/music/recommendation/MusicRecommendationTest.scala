package music.recommendation

import music.recommendation.core.MusicRecommendation
import music.recommendation.utility.{FileLocations, SparkUtility}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by deveshkandpal on 4/21/17.
  */
class MusicRecommendationTest extends FlatSpec with Matchers with BeforeAndAfter {

  before {
    SparkUtility.spark
  }
  val spark = SparkUtility.spark
  import spark.implicits._
  behavior of "SongInfo Parsing"
  it should "correctly parse into RDD[SongInfo]" in {
    val testRow = spark.sparkContext.parallelize(List("TRAAAAW128F429D538<SEP>SOMZWCG12A8C13C480<SEP>Casual<SEP>I Didn't Mean To")).toDF().rdd
    val parsedSongInfo = MusicRecommendation.convertToSongInfo(testRow)
    val collectedRdd = parsedSongInfo.flatMap(f => f.map(q => q)).collect()
    collectedRdd.length shouldBe(1)
    collectedRdd.map(m => m.trackId shouldBe("TRAAAAW128F429D538"))
  }

  behavior of "User Test Parsing"
  it should "correctly parse into RDD[UserTaste]" in {
    val testRow = spark.sparkContext.parallelize(List("b80344d063b5ccb3212f76538f3d9e43d87dca9e\tTRAAAAW128F429D538\t1")).toDF().rdd
    val parsedUserTaste = MusicRecommendation.convertToUserSongTaste(testRow)
    parsedUserTaste.collect().map( m => m.userId shouldBe("b80344d063b5ccb3212f76538f3d9e43d87dca9e"))
  }

  behavior of "Parse Method"
  it should "correctly parse a string" in {
    val str = "b80344d063b5ccb3212f76538f3d9e43d87dca9e,1"
    val out = MusicRecommendation.parse(str)
    assert(out.isFailure)
  }

  behavior of "Test KMeans Model Existence"
  it should "return failure for failing to load KMeans Model" in {
    val out = MusicRecommendation.loadKMeansModel(spark, "Wrong Location Provided")
    assert(out.isFailure)


  }

  behavior of "Word Dictionary Parsing"
  it should "correctly parsed word dictionary dataset" in {
    val in = spark.sparkContext.parallelize(List("the")).toDF().rdd
    MusicRecommendation.convertToDict(in).collect().head shouldBe((1,"the"))
  }

  behavior of "Parse Lyrics Info RDD correctly"
  it should "correctly parse into RDD[LyricsInfo]" in {
    val dict = Map[Int, String](1 -> "the")
    val in = spark.sparkContext.parallelize(List("TRAAAAV128F421A322,4623710,1:6")).toDF().rdd
    MusicRecommendation.convertToLyricsInfo(in, dict).collect()(0).trackId shouldBe("TRAAAAV128F421A322")
  }

  behavior of "Reverse zip should be reversed correctly"
  it should "it should reverse key value pairs" in {
    val dict = scala.collection.immutable.Map[String, Long]("apple" -> 1L)
    val p = MusicRecommendation.reverseZip(dict)
    assert(p.keys.toList.contains(1L))
  }

behavior of "Conversion of Dataframe to RDD[ArtistCount]"
  it should "correctly parse dataframe columns to RDD[ArtistCount]" in {
    val in = spark.sparkContext.parallelize(List(("U1", "A1", 10L))).toDF("userId", "artist", "count")
    val out = MusicRecommendation.convertCombinedDfToRdd(in)
    out.collect()(0).artist shouldBe("A1")
  }

  behavior of "Test for Artist Zip"
  it should "correctly parse into artist map" in {
    val in = spark.sparkContext.parallelize(List(("U1", "A1", 10L))).toDF("userId", "artist", "count")
    val rdd = MusicRecommendation.convertCombinedDfToRdd(in)
    val out = MusicRecommendation.prepareArtistZip(rdd)
    out("A1") shouldBe(0)
  }


  behavior of "Test for User Zip"
  it should "correctly parse into user map" in {
    val in = spark.sparkContext.parallelize(List(("U1", "A1", 10L))).toDF("userId", "artist", "count")
    val rdd = MusicRecommendation.convertCombinedDfToRdd(in)
    val out = MusicRecommendation.prepareUserZip(rdd)
    out("U1") shouldBe(0)
  }



}
