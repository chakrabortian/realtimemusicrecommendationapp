package actors

import actors.KafkaActor.Subscribe
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

import models.Recos
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



   // while(true) {

  val rec = KafkaActor.consumer.poll(1000)
  println("Total records : " + rec.count())

  val iter = rec.iterator()
      while(iter.hasNext) {
        val conRec = iter.next()
        println("con rec : " + conRec.key() + " value : " + conRec.value())
        val userId = conRec.key()
        val recos = conRec.value()

        KafkaActor.userActorMap(userId) ! Recos(recos)


      }

//}




    }
    case message : String => {
      Logger.info("Message received by kafka" + message)
        sendMessageToKafka(message, sender())

    }
  }

  def sendMessageToKafka(message : String, sender : ActorRef) = {
    Logger.info("message : " + message)
    val messageArr = message.split("<SEP>")
    val userId  = messageArr(0)
    val kafkaMessage = messageArr.drop(1)
    KafkaActor.userActorMap = KafkaActor.userActorMap ++ Map[String, ActorRef](userId -> sender)


    val  properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("acks","1")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](properties)

    val topic="artistrecommendation"

    val record = new ProducerRecord(topic, userId, kafkaMessage.mkString("<SEP>"))
    producer.send(record)

    // self ! Subscribe
    Logger.info("Message sent to kafka!!")
    sender ! ("Message sent to kafka", message)
  }



}

object KafkaActor {

  case object Subscribe

  var userActorMap =  Map[String, ActorRef]()

  def props() = Props(new KafkaActor())


  println("Subscribe message sent")

  val  properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  properties.put("group.id", "test-consumer-group")
  properties.put("auto.offset.reset", "latest")





  val consumer = new KafkaConsumer[String, String](properties)

  consumer.subscribe(Collections.singletonList("generatedartistrecommendations"))


}
