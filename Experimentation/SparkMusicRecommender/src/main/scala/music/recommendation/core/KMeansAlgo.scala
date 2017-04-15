package music.recommendation.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans

/**
  * Created by chakr on 4/8/2017.
  */
class KMeansAlgo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    val dataset = spark.read.text("C:\\Coursera_Spark\\Recommendation_System_Scala\\train_triplets.txt")

    val feature_data = dataset

    import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer, VectorIndexer, OneHotEncoder}
    import org.apache.spark.ml.linalg.Vectors




  }

}
