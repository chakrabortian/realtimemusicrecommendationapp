package music.recommendation.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.clustering.KMeans

/**
  * Created by chakr on 4/8/2017.
  */

  object KMeans_Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val dataset = spark.read.option("header","true").option("inferSchema","true").csv("Wholesale customers data.csv")

    val feature_data = dataset.select($"Fresh", $"Milk", $"Grocery", $"Frozen", $"Detergents_Paper", $"Delicassen")

    val assembler = new VectorAssembler().setInputCols(Array("Fresh", "Milk", "Grocery", "Frozen", "Detergents_Paper", "Delicassen")).setOutputCol("features")

    // Use the assembler object to transform the feature_data
    // Call this new data training_data
    val training_data = assembler.transform(feature_data).select("features")

    // Create a Kmeans Model with K=3
    val kmeans = new KMeans().setK(3).setSeed(1L)

    // Fit that model to the training_data
    val model = kmeans.fit(training_data)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(training_data)
    println(s"Within Set Sum of Squared Errors = $WSSSE")


    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

      model.summary.predictions.show(20)

  }
}
