package actors

import actors.KafkaActor.{LyricsSongRecommendation, SimilarArtistRecommendation}
import akka.actor.{Actor, ActorRef, Props}
import models.{ArtistRecos, Done}
import play.api.libs.json.Json

/**
  * Created by deveshkandpal on 4/20/17.
  */
class SongLyricsActor(out : ActorRef, kafkaActor : ActorRef) extends Actor {

  override def receive = {

    case ArtistRecos(r) => {
      val obj = Json.obj(
        "status" -> "OK",
        "artist" -> r
      ).toString()

      out ! obj
    }
    case Done =>   println("Message Sent to Kafka for Lyrics Recommendation")
    case message : String =>   kafkaActor ! (LyricsSongRecommendation, message)

  }

}

object SongLyricsActor {

  def props(out : ActorRef, kafkaActor : ActorRef) = Props(new SongLyricsActor(out, kafkaActor))
}
