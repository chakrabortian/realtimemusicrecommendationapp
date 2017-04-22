package music.recommendation.bo

import org.apache.spark.mllib.linalg.{Vector}

/**
  * Created by priyesh on 4/21/2017.
  */
case class SongVectorPrediction(id : String, vector: Vector, prediction: Int)
