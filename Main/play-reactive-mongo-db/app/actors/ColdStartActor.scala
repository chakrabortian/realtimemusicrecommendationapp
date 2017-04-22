package actors

import actors.KafkaActor.ColdStartArtistRecommendation
import akka.actor.{Actor, ActorRef, Props}
import models.{ArtistRecos, Done}
import play.api.{Logger, Play}
import play.api.libs.iteratee.{Concurrent, Enumeratee, Enumerator, Iteratee}
import play.api.libs.json.{JsObject, Json}
import play.api.libs.ws.WS
import play.extras.iteratees.{Encoding, JsonIteratees}
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits._

import scala.collection.immutable.Map

/**
  * Created by deveshkandpal on 4/17/17.
  */
class ColdStartActor(out : ActorRef, kafkaActor : ActorRef) extends Actor {

  override def receive = {

    case ArtistRecos(r) => {


      val obj = Json.obj(
        "status" -> "OK",
        "artist" -> r
      ).toString()
      out ! obj

    }
    case Done =>   println("Cold Start Request sent to Kafka")
    case message : String =>   {
      println("Cold start request received")
      kafkaActor ! (ColdStartArtistRecommendation, message)
    }

  }

}

object ColdStartActor {
  def props(out : ActorRef, kafkaActor : ActorRef) = Props(new ColdStartActor(out, kafkaActor))
}
