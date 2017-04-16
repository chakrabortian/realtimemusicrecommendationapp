package music.recommendation.core


import org.apache.spark.sql.functions._
import music.recommendation.utility.NLPUtility
import org.apache.spark.sql.SparkSession



/**
  * Created by priyeshkaushik on 4/15/17.
  *
  */
object LyricsAnalysis {

  def main(args : Array[String]): Unit = {

    val spark : SparkSession = SparkSession
      .builder().master("local")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "7g")
      .config("spark.executor.memory", "3g")
      .getOrCreate()


    import spark.sqlContext.implicits._

    val str1 = Seq("It was an excellent movie", "good was it").toDF("words")

    str1.select(NLPUtility.sentiment('words).as("pos")).show()





  }

}
