package actors

import actors.KafkaActor._
import akka.actor.{Actor, ActorRef, Props}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import play.api.{Logger, Play}
import play.api.libs.iteratee.{Concurrent, Enumeratee, Enumerator, Iteratee}
import play.api.libs.json.{JsObject, Json}
import play.api.libs.ws.WS
import play.extras.iteratees.{Encoding, JsonIteratees}
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits._
import java.util.{Collections, Properties}

import actors.ChildActor.Done
import models.ArtistRecos
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.immutable.Map
import scala.concurrent.Future


/**
  * Created by deveshkandpal on 4/17/17.
  */
class KafkaActor extends Actor {






  override def receive = {

    case Subscribe => {
        pollKafkaForIncomingMessage
    }
    case (ColdStartArtistRecommendation, message : String) => {
      sendColdStartMessageToKafka(message, sender())

    }
    case (SimilarArtistRecommendation, message : String) => {

    }
    case (LyricsSongRecommendation, message : String) => {

    }
    case (LyricsUserRecommendation, message : String) => {

    }
  }

  def sendColdStartMessageToKafka(message : String, sender : ActorRef) = {
    val messageArr = message.split("<SEP>")
    val userId  = messageArr(0)
    val kafkaMessage = messageArr.drop(1)
    userActorMap = userActorMap ++ Map[String, ActorRef](userId -> sender)
    val record = new ProducerRecord("artistrecommendation", userId, kafkaMessage.mkString("<SEP>"))
    producer.send(record)
    sender ! Done
  }


  def pollKafkaForIncomingMessage = {
    val rec = consumer.poll(1000)
    println("Total records : " + rec.count())
    val iter = rec.iterator()
    while(iter.hasNext) {
      val conRec = iter.next()
      println("Key : " + conRec.key() + " Value : " + conRec.value())
      val userId = conRec.key()
      val recommendation = conRec.value()
      userActorMap(userId) ! ArtistRecos(recommendation)
      userActorMap = userActorMap.filter(f => !f._1.equalsIgnoreCase(userId))
    }
  }

}

object KafkaActor {

  case object Subscribe
  case object ColdStartArtistRecommendation
  case object SimilarArtistRecommendation
  case object LyricsSongRecommendation
  case object LyricsUserRecommendation

  var userActorMap =  Map[String, ActorRef]()

  def props() = Props(new KafkaActor())

  val  properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  properties.put("group.id", "test-consumer-group")
  properties.put("auto.offset.reset", "latest")

  val producer = new KafkaProducer[String, String](properties)
  val consumer = new KafkaConsumer[String, String](properties)

  consumer.subscribe(Collections.singletonList("generatedartistrecommendations"))


}
