package music.recommendation.ingestion

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by deveshkandpal on 4/18/17.
  */
object DataInjestion {

  def readData(spark : SparkSession, location : String) : RDD[Row] = spark.read.text(location).rdd


}
