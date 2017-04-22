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
import models.{ArtistRecos, Done}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringDeserializer
import collection.JavaConverters._

import scala.collection.mutable.Map
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
    case (ExistingUserRecommendation, message : String) => {
      sendExistingUserRecoRequestToKafka(message, sender())
    }
    case (SimilarArtistRecommendation, message : String) => {
      sendArtistForArtistRecoRequestToKafka(message, sender)
    }
    case (LyricsSongRecommendation, message : String) => {
      sendLyricsSongRecommendationRequestToKafka(message, sender)
    }
  }

  def sendLyricsSongRecommendationRequestToKafka(message : String, sender : ActorRef) = {
    val messageArr = message.split("<SEP>")
    val userId  = messageArr(0)
    val kafkaMessage = messageArr.drop(1)
    songsForUserArtistMap += (userId -> sender)
    println("SENT REQUEST FOR LYRICS SONG")
    val record = new ProducerRecord(LYRICSUSER_TOPIC_REQUEST, userId, kafkaMessage.mkString("<SEP>"))
    producer.send(record)
    sender ! Done

  }

  def sendArtistForArtistRecoRequestToKafka(message: String, sender : ActorRef) = {
    val messageArr = message.split("<SEP>")
    val userId  = messageArr(0)
    val kafkaMessage = messageArr.drop(1)
    artistForArtistMap += (userId -> sender)
    println("SENT REQUEST FOR ARTIST-ARTIST")
    val record = new ProducerRecord(ARTISTFORARTIST_TOPIC_REQUEST, userId, kafkaMessage.mkString("<SEP>"))
    producer.send(record)
    sender ! Done
  }

  def sendExistingUserRecoRequestToKafka(message : String, sender : ActorRef) = {
    println("SENT REQUEST FOR EXISTING USER")
    existingUserMap += (message -> sender)
    val record = new ProducerRecord(EXISTINGUSER_TOPIC_REQUEST, message, message)
    producer.send(record)
    sender ! Done
  }

  def sendColdStartMessageToKafka(message : String, sender : ActorRef) = {
    val messageArr = message.split("<SEP>")
    val userId  = messageArr(0)
    val kafkaMessage = messageArr.drop(1)
    coldStartUserMap += (userId -> sender)
    println("SENT REQUEST FOR COLD START")
    val record = new ProducerRecord(COLDSTARTRECO_TOPIC_REQUEST, userId, kafkaMessage.mkString("<SEP>"))
    producer.send(record)
    sender ! Done
  }


  def pollKafkaForIncomingMessage = {
    val rec = consumer.poll(1000)
    println("Total records : " + rec.count())
    val iter = rec.iterator()
    while(iter.hasNext) {
      val consumerRecord = iter.next()
      println("Key : " + consumerRecord.key() + " Value : " + consumerRecord.value())
      val userId = consumerRecord.key()
      val recommendation = consumerRecord.value()
      consumerRecord.topic() match {
        case COLDSTARTRECO_TOPIC_RESPONSE => {
          println("RECEIVED RESPONSE FOR COLD START")
          coldStartUserMap(userId) ! ArtistRecos(recommendation)
          coldStartUserMap.remove(userId)
        }
        case ARTISTFORARTIST_TOPIC_RESPONSE => {
          println("RECEIVED RESPONSE FOR ARTIST TO ARTIST")
        artistForArtistMap(userId) ! ArtistRecos(recommendation)
          artistForArtistMap.remove(userId)

         }
        case EXISTINGUSER_TOPIC_RESPONSE => {
          println("RECEIVED RESPONSE FOR EXISTING USER")
          existingUserMap(userId) ! ArtistRecos(recommendation)
          existingUserMap.remove(userId)
        }
        case LYRICSUSER_TOPIC_RESPONSE => {
          println("RECEIVED RESPONSE FOR LYRICS SONG")
          songsForUserArtistMap(userId) ! ArtistRecos(recommendation)
          songsForUserArtistMap.remove(userId)
        }
      }

    }
  }

}

object KafkaActor {

  case object Subscribe
  case object ColdStartArtistRecommendation
  case object SimilarArtistRecommendation
  case object LyricsSongRecommendation
  case object ExistingUserRecommendation

  val COLDSTARTRECO_TOPIC_REQUEST = "ColdStartRecoRequest"
  val ARTISTFORARTIST_TOPIC_REQUEST = "ArtistforArtistRecoRequest"
  val EXISTINGUSER_TOPIC_REQUEST = "ExistingUserRecoRequest"
  val LYRICSUSER_TOPIC_REQUEST = "SongRecoRequest"

  val COLDSTARTRECO_TOPIC_RESPONSE = "ColdStartRecoResponse"
  val ARTISTFORARTIST_TOPIC_RESPONSE = "ArtistforArtistRecoResponse"
  val EXISTINGUSER_TOPIC_RESPONSE = "ExistingUserRecoResponse"
  val LYRICSUSER_TOPIC_RESPONSE = "SongRecoResponse"

  val coldStartUserMap =  Map[String, ActorRef]()
  val existingUserMap =  Map[String, ActorRef]()
  val artistForArtistMap =  Map[String, ActorRef]()
  val songsForUserArtistMap = Map[String, ActorRef]()

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

  val l = List(COLDSTARTRECO_TOPIC_RESPONSE,
    ARTISTFORARTIST_TOPIC_RESPONSE, EXISTINGUSER_TOPIC_RESPONSE, LYRICSUSER_TOPIC_RESPONSE).asJava

  consumer.subscribe(l)

}
