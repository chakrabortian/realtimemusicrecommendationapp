package actors


import actors.KafkaActor.{ColdStartArtistRecommendation, SimilarArtistRecommendation}
import akka.actor.{Actor, ActorRef, Props}
import models.{ArtistRecos, Done}
import play.api.libs.json.Json

/**
  * Created by deveshkandpal on 4/20/17.
  */
class ArtistForArtistActor(out : ActorRef, kafkaActor : ActorRef) extends Actor {

  override def receive = {

    case ArtistRecos(r) => {
      val obj = Json.obj(
        "status" -> "OK",
        "artist" -> r
      ).toString()

      out ! obj
    }
    case Done => println("ARTIST FOR ARTIST REQUEST SUCCESSFULLY SHIPPED TO KAFKA")

    case message : String =>   kafkaActor ! (SimilarArtistRecommendation, message)

  }
}

object ArtistForArtistActor {

  def props(out : ActorRef, kafkaActor : ActorRef) = Props(new ArtistForArtistActor(out, kafkaActor))
}
