package music.recommendation.core

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import kafka.producer.ProducerConfig
import music.recommendation.bo.{LyricsInfo, UserSongLyricsPosInfo}
import music.recommendation.utility.FileLocations
import org.apache.commons.lang.StringEscapeUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import scala.collection.immutable.HashMap

/**
  * Created by deveshkandpal on 4/17/17.
  */
object Dummy {

  def main(args : Array[String]) : Unit = {



    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Real Time Music Reco App")
      .config("spark.driver.memory", "7g")
      .config("spark.executor.memory", "3g")
      .getOrCreate()

    val sc = spark.sparkContext

    import org.bson.Document

    val writeConfig = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/musicrecoapp.users"))

    val p = List("devesh's", "kand").mkString("<SEP>")
    val str = StringEscapeUtils.escapeXml(p)
    val uId = "dk"

    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"""{username: '$uId', artist : '$str'}""")))


    MongoSpark.save(documents,writeConfig)








    //    val a = "value:NEW<SEP>A1<SEP>A2<SEP>A3"
    //    val b = a.substring(6, a.length).split("<SEP>").drop(1)
    //    println(b.mkString(","))

    //    val a = UserSongLyricsPosInfo("devesh", "1", "1", 1, 10, "Adj", 10)
    //    val b = UserSongLyricsPosInfo("devesh", "1", "1", 1, 9, "Verb", 11)
    //    val c = UserSongLyricsPosInfo("devesh", "2", "1", 1, 9, "Verb", 9)
    //    val d = UserSongLyricsPosInfo("devesh", "2", "1", 1, 10, "Adj", 12)
    //    val e = UserSongLyricsPosInfo("devesh", "3", "1", 1, 6, "Interjection", 1)


    //    val a = LyricsInfo("1", 1, 10, "A")
    //    val b = LyricsInfo("1", 2, 11, "V")
    //    val c = LyricsInfo("1", 3, 12, "Ad")
    //
    //    val d = LyricsInfo("2", 4, 9, "A")
    //    val e = LyricsInfo("2", 5, 4, "V")
    //     val f = LyricsInfo("2", 6, 6, "Ad")
    //
    //     val g = LyricsInfo("3", 7, 1, "A")
    //    val h = LyricsInfo("3", 8, 6, "V")
    //    val i = LyricsInfo("3", 9, 12, "Ad")
    //
    //
    //    val l = List(a,b,c,d,e,f,g,h,i)

    //    val q = l.groupBy(us2 => us2.pos).map(us3 => us3._2.sortWith((l1, l2) => l1.count > l2.count).take(2)).flatMap(fm => fm).groupBy(gb => gb.pos).map(mv => {
    //      if(mv._2.size < 2) {
    //        val zerosToBeAdded = 2 - mv._2.size
    //        val zerosList = List.fill(zerosToBeAdded)(LyricsInfo("",0,0,mv._1))
    //        val newMv = List.concat(mv._2, zerosList)
    //        (mv._1, newMv)
    //      } else (mv._1, mv._2)
    //    }).flatMap(a => a._2)


    //    val p = l.groupBy(song => song.trackId).map(sl => (sl._1,sl._2.groupBy(us2 => us2.pos).map(us3 =>(us3._1,us3._2
    //      .sortWith((l1, l2) => l1.count > l2.count)
    //      .take(2))).map(mv => {
    //      if(mv._2.size < 2) {
    //        val zerosToBeAdded = 2 - mv._2.size
    //        val zerosList = List.fill(zerosToBeAdded)(LyricsInfo("",0,0,mv._1))
    //        val newMv = List.concat(mv._2, zerosList)
    //        (newMv)
    //      } else (mv._2)
    //    })))
    //      .map(m => Vectors.dense(m._2.flatMap(a=>a).map(e => e.wordId.toDouble).toArray)).filter(v => v.size == 6)


    //      val q = p
    //      .flatMap(a => a._2).groupBy(s => s.trackId)
    //      .map(m => Vectors.dense(m._2.map(e => e.wordId.toDouble).toArray))






    //    val q = l.groupBy(us => us.userId).flatMap(us1 => us1._2.groupBy(us2 => us2.pos).map(us3 => us3._2.sortWith((a, b) => a.wordCount > b.wordCount).take(1))).flatMap(a => a).groupBy(a => a.userId).filter(p => p._2.size == 2).flatMap(a => a._2)






    //    p.toList.foreach(a => println(a))
    //println("aaaaaaaa")




  }

}
