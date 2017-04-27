package music.recommendation

import music.recommendation.algorithms.GenerateRecommendations
import music.recommendation.utility.SparkUtility
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by Anindita on 4/20/17.
  */
class GenerateRecommendationTest extends FlatSpec with Matchers with BeforeAndAfter {

  before {
    SparkUtility.spark
  }
  val spark = SparkUtility.spark
  import spark.implicits._

  behavior of "Test for Euclidean Distance calculation"
  it should "return the distance between two vectors" in {
    val v1 = Vectors.dense(Array[Double](1.0,2.0,3.0))
    val v2 = Vectors.dense(Array[Double](2.0,3.0,4.0))
    val distance = GenerateRecommendations.getDistance(v1, v2)
    distance shouldBe(Math.sqrt(3))

  }

}
