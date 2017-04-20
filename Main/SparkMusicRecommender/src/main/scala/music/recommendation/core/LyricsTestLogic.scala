package music.recommendation.core

import music.recommendation.utility.NLPUtility
import org.apache.spark.sql.SparkSession

/**
  * Created by deveshkandpal on 4/16/17.
  */
object LyricsTestLogic {

  def main(args : Array[String]) = {

    val spark : SparkSession = SparkSession
      .builder().master("local")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "7g")
      .config("spark.executor.memory", "3g")
      .getOrCreate()


    import spark.sqlContext.implicits._



    val s = "love"
    val w = Seq(s).toDF("words")

    w.select(NLPUtility.getPartOfSpeech('words)).show()



  }

}
