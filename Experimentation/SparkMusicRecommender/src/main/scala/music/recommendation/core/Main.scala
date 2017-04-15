package music.recommendation.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.{SQLContext, SparkSession}


/**
  * Created by deveshkandpal on 4/5/17.
  */

object Main {

  def main(args : Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("K Means").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sQLContext = new org.apache.spark.sql.SQLContext(sc)

    //val rows = sc.textFile(DATAFILE).flatMap(_.split(',').map(_.toDouble)).cache()

    val data = sc.textFile("C:\\Coursera_Spark\\Recommendation_System_Scala\\train_triplets.txt")
    val Array(training, test) = data.randomSplit(Array(0.7,0.3), seed = 12345)
    val trainingData = convertToUserSongTaste(training)
    val testingData = convertToUserSongTaste(test)


    val testDataZip = prepareUserZip(testingData)
    val trainDataZip = prepareUserZip(trainingData)

    val assignTrainData = trainData.map(p => CC1(p(0).toDouble, p(1).toDouble, p(2).toDouble))
    val assignTestData = testData.map(p => CC1(p(0).toDouble, p(1).toDouble, p(2).toDouble ))


    import sQLContext.implicits._

    //Convert data RDD to dataframe
    val trainingDataFrame = assignTrainData.toDF()
    val testDataFrame = assignTestData.toDF()

    println("Training Data Snapshot: ")
    trainingDataFrame.show()

    println("Test Data Snapshot: ")
    testDataFrame.show()

    val rowsRDD = trainingDataFrame.rdd.map(r => (r(0), r(1), r(2)))
    rowsRDD.cache()

    val rowsTestRDD = testDataFrame.rdd.map(r => (r(0), r(1), r(2)))




    //Training dataset vector

    val trainVector = trainingDataFrame.rdd.map(r => Vectors.dense(r.get(0).asInstanceOf[Double], r.get(1).asInstanceOf[Double], r.get(0).asInstanceOf[Double]))

   val testVector = testDataFrame.rdd.map(r => Vectors.dense(r.get(0).asInstanceOf[Double], r.get(1).asInstanceOf[Double], r.get(0).asInstanceOf[Double]))

//    val trainVector = trainingDataFrame.rdd.map(r => Vectors.dense(r.getDouble(0), r.getDouble(1), r.getDouble(2)))

 //   val testVector = testDataFrame.rdd.map(r => Vectors.dense(r.getDouble(0), r.getDouble(1), r.getDouble(2)))

    println("Test Vector: ")
    testVector.foreach(println)

    val numOfClusters = 3
    val numOfIterations = 10

    val trainingSet = KMeans_Test.train(trainVector, numOfClusters, numOfIterations)
    trainingSet.clusterCenters.foreach(println)   //Prints each cluster starting with Cluster
    println(trainingSet.computeCost(trainVector))     //Prints sum of squared errors


    trainingSet.save(sc, "C:\\KMeans_Model")

    val model = KMeansModel.load(sc, "C:\\KMeans_Model")
    val resultRDD = model.predict(testVector)

    val resultDataFrame = resultRDD.toDF()

    resultDataFrame.show()



    sc.stop()
  }

  case class CC1(userId: Double, songId: Double, playCount: Double)

  def convertToUserSongTaste(rawRdd : RDD[String]) : RDD[UserTaste] = rawRdd.map(row => {
    val x = row.split("\\W")
    UserTaste(x(0).trim,x(1).trim,x(2).trim.toInt)
  })
  case class UserTaste(userId : String, songId : String, count : Int)

  def prepareUserZip(data: RDD[UserTaste])
  : Map[String,Long] = data.map(a => a.userId).distinct()
    .sortBy(x => x).zipWithIndex.collectAsMap()

  }