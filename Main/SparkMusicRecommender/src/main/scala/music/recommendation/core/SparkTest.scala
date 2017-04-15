package music.recommendation.core

import org.apache.spark.sql.SparkSession

/**
  * Created by deveshkandpal on 4/14/17.
  */
object SparkTest {

  def main(args : Array[String]) : Unit = {


    val spark : SparkSession = SparkSession
      .builder().master("local")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "7g")
      .config("spark.executor.memory", "3g")
      .getOrCreate()


    import spark.implicits._





  }

}
