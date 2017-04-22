package actors

import actors.KafkaActor.ExistingUserRecommendation
import akka.actor.{Actor, ActorRef, Props}
import models.{ArtistRecos, Done}
import play.api.libs.json.Json

/**
  * Created by deveshkandpal on 4/20/17.
  */
class ExistingUserActor(out : ActorRef, kafkaActor : ActorRef) extends Actor {

  override def receive = {

    case ArtistRecos(r) => {
      val obj = Json.obj(
        "status" -> "OK",
        "artist" -> r
      ).toString()

      out ! obj
    }
    case Done =>   println("Existing use successfully pushed to Kafka")
    case message : String =>   {
      println("Socket request received for user : " + message)
      kafkaActor ! (ExistingUserRecommendation, message)
    }
  }
}

object ExistingUserActor {

  def props(out : ActorRef, kafkaActor : ActorRef) = Props(new ExistingUserActor(out, kafkaActor))
}
