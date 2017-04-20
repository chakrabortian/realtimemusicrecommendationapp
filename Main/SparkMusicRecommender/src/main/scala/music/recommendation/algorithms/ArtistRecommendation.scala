package music.recommendation.algorithms

import music.recommendation.bo.ArtistCount
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import music.recommendation.bo._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import music.recommendation.utility.FileLocations._
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

import scala.collection.Map
import scala.util.{Failure, Success, Try}

/**
  * Created by deveshkandpal on 4/18/17.
  */
class ArtistRecommendation {

  def prepareArtistRecommendation(spark : SparkSession, combinedRdd : RDD[ArtistCount], userZip : Map[String, Long],
                                 artistZip : Map[String, Long]) = {

    import spark.implicits._

    val groupedFeatureVector = getGroupedFeatureVector(combinedRdd)
    val flattennedFeatureVector = flattenRDD(groupedFeatureVector)



    spark.sparkContext.broadcast(userZip)
    spark.sparkContext.broadcast(artistZip)

    val artistGroupedByUser = convertArtistNameToIndex(flattennedFeatureVector.toDF(), artistZip)
      .groupBy(u => u.userId)


    val userVectorMapping = getUserVectorMapping(artistGroupedByUser)

    val parsedData = userVectorMapping.map(entry => entry._2)

    val clusters = getKMeansArtistModel(parsedData , spark, ARTIST_K_MEANS_MODEL_TRAIN_LOCATION)

    checkOrTrainALSModels(spark , parsedData, clusters, userVectorMapping, userZip,
      artistZip, combinedRdd, ALS_MODEL_TRAIN_LOCATION)

    clusters


  }

  def checkOrTrainALSModels(spark: SparkSession, parsedData : RDD[Vector],
                            clusters : KMeansModel,
                            userVectorMapping : RDD[(String, Vector)],
                            userZip : Map[String, Long],
                            artistZip: Map[String, Long],
                            combinedRdd : RDD[ArtistCount],
                            alsLocation : String) = {

    val loadedModel = Try(MatrixFactorizationModel.load(spark.sparkContext, alsLocation+ "0"))

    loadedModel match {
      case Success(model) => println("ALS Model present, no need to train")
      case Failure(x) => {
        println("ALS Model not present, need to train")
        trainArtistALSModels(spark , parsedData, clusters, userVectorMapping, userZip,
          artistZip, combinedRdd, alsLocation)
      }
    }
  }

  def trainArtistALSModels(spark: SparkSession, parsedData : RDD[Vector],
                           clusters : KMeansModel,
                           userVectorMapping : RDD[(String, Vector)],
                           userZip : Map[String, Long],
                           artistZip: Map[String, Long],
                           combinedRdd : RDD[ArtistCount],
                           alsLocation : String) : Unit = {

    import spark.implicits._

    val vectorPredictionMapping = parsedData.map(vector => {
      val prediction = clusters.predict(vector)
      (vector, prediction)
    })

    val userVectorMappingDf = userVectorMapping.toDF("userId", "vector")
    val vectorPredictionMappingDf = vectorPredictionMapping.toDF("vector", "prediction")

    val joinedUserPredictionMappingDf = userVectorMappingDf.join(vectorPredictionMappingDf, "vector")

    val userPredictionRDD  = joinedUserPredictionMappingDf.rdd.map(row => UserPrediction(row.getAs("userId"), row.getAs("prediction")))

    val ratings = convertCombinedRddToRating(combinedRdd, userZip, artistZip)

    //val reversedUserZip = reverseUserZip(userZip)

    val userIdPredictionRdd = userPredictionRDD.map(upRdd => (userZip(upRdd.userId).toInt, upRdd.prediction))

    val userIdPredictionDf = userIdPredictionRdd.toDF("user", "clusterId")

    val alsKMeansDf = userIdPredictionDf.join(ratings.toDF(), "user")

    val alsRatingRDD = alsKMeansDf.rdd.map(alsr => (Integer.valueOf(alsr.getAs("clusterId").toString), Rating(alsr.getAs("user"), alsr.getAs("product"), alsr.getAs("rating"))))

    val collected = alsRatingRDD.groupByKey().collectAsMap()

    spark.sparkContext.broadcast(collected)

    for((k, v) <- collected) {

      val clusterId = k
      val filteredRating = spark.sparkContext.parallelize(v.toList)
      val model = ALS.trainImplicit(filteredRating, 10, 5, 0.01, 1.0)
      model.save(spark.sparkContext,  alsLocation + clusterId)
      println("Model Saved at : " + alsLocation + clusterId)

    }


  }

  def convertCombinedRddToRating(data : RDD[ArtistCount], users : Map[String, Long], artists : Map[String, Long]) : RDD[Rating] = data.map(r => Rating(users(r.userId).toInt, artists(r.artist).toInt, r.count))



  def getKMeansArtistModel(parsedData: RDD[Vector], spark: SparkSession, location : String) : KMeansModel  = {
    val loadModel = Try(KMeansModel.load(spark.sparkContext, location))
    loadModel match {
      case Success(model) => {
        println("KMeans Artist Model found")
        model
      }
      case Failure(x) =>  {
        println("KMeans Artist Model Not Found, Training KMeans Artist")
        trainKMeansArtist(parsedData, spark, location)
      }
    }
  }
  def trainKMeansArtist(parsedData : RDD[Vector], spark : SparkSession, location : String) : KMeansModel = {

    val numClusters = 12
    val numIterations = 30
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val wssse = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors for Artist Cluster with cluster size 12 is : " + wssse)
    clusters.save(spark.sparkContext, location)

    clusters
  }

  def getVectorPredictionMapping(parsedData : RDD[Vector], clusters : KMeansModel) = parsedData.map(vector => {
    val prediction = clusters.predict(vector)
    (vector, prediction)
  })

  def getUserVectorMapping(artistGroupedByUser : RDD[(String, Iterable[UserArtist])]) : RDD[(String, Vector)] = artistGroupedByUser.map(entry => {
    val vector = Vectors.dense(entry._2.map(ua => ua.artistId.toDouble).toArray)
    (entry._1, vector)
  }).cache()

  def convertArtistNameToIndex(df: DataFrame, artistZip: Map[String, Long]): RDD[UserArtist] = df
    .rdd.
    map(row => UserArtist(row.getAs("userId"), artistZip(row.getAs("artist"))))

  def flattenRDD(input: RDD[List[ArtistCount]]): RDD[ArtistCount] = input.flatMap(a => a.map(b => b))

  def getGroupedFeatureVector(combinedRdd : RDD[ArtistCount]) = combinedRdd
    .groupBy(k => k.userId).filter(f => f._2.size >= 3)
    .map(record => record._2.toList)
    .map(newRec => newRec.sortWith((fe, se) => fe.count > se.count)
      .dropRight(newRec.size - 3))









}
