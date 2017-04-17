package music.recommendation.utility



import scala.collection.JavaConverters._

import edu.stanford.nlp.simple.{ Sentence}


import org.apache.spark.sql.functions.udf


/**
  * Created by priyesh on 4/15/17.
  * @Credits - https://github.com/databricks/spark-corenlp
  */
object NLPUtility {

  /**
    * Generates the part of speech tags of the sentence.
    * @see [[Sentence#posTags]]
    */
  def getPartOfSpeech = udf { word: String =>
    new Sentence(word).posTags().asScala
  }

}
