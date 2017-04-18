package actors

import akka.actor.{Actor, ActorRef, Props}
import play.api.{Logger, Play}
import play.api.libs.iteratee.{Concurrent, Enumeratee, Enumerator, Iteratee}
import play.api.libs.json.{JsObject, Json}
import play.api.libs.ws.WS
import play.extras.iteratees.{Encoding, JsonIteratees}
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits._
import utils.Producer

import scala.collection.immutable.Map
import scala.concurrent.Future

/**
  * Created by deveshkandpal on 4/17/17.
  */
class KafkaActor extends Actor {

  //New<SEP>userId<SEP>Artist1<SEP>Artist2<SEP>Artist3
  override def receive = {
    case message : String => {
      Logger.info("Message received by kafka" + message)
        sendMessageToKafka(message, sender())

    }
  }

  def sendMessageToKafka(message : String, sender : ActorRef) = {
    Logger.info("message : " + message)
    val userId  = message.split("<SEP>")(1)
    KafkaActor.userActorMap = KafkaActor.userActorMap ++ Map[String, ActorRef](userId -> sender)

    import java.util.Properties

    import org.apache.kafka.clients.producer._

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks","1")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val topic="kafkatopic"

    val record = new ProducerRecord(topic, "key : "+userId, "value:"+message)
    producer.send(record)

    Logger.info("Message sent to kafka!!")
    sender ! ("Message sent to kafka", message)
  }

}

object KafkaActor {

  var userActorMap =  Map[String, ActorRef]()

  def props() = Props(new KafkaActor())
}
