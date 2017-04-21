package actors

import actors.ChildActor.Done
import actors.KafkaActor.ColdStartArtistRecommendation
import akka.actor.{Actor, ActorRef, Props}
import models.ArtistRecos
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
class ChildActor(out : ActorRef, kafkaActor : ActorRef) extends Actor {

  override def receive = {

    case ArtistRecos(r) => out ! r
    case Done => {
      out ! "Done"
    }
    case message : String => {
      kafkaActor ! (ColdStartArtistRecommendation, message)
    }
  }

}

object ChildActor {
  case object Done
  def props(out : ActorRef, kafkaActor : ActorRef) = Props(new ChildActor(out, kafkaActor))
}
