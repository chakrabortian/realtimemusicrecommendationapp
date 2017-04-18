package actors

import akka.actor.{Actor, ActorRef, Props}
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
    case (a, b) => {
      Logger.info("Message received by Child from Kafka")
      out ! "Done!" + b
    }
    case message : String => {
      Logger.info("Message received by child and now sending to kafka")
      kafkaActor ! message
    }


  }

}

object ChildActor {

  val userActorMap =  Map[String, ActorRef]()

  def props(out : ActorRef, kafkaActor : ActorRef) = Props(new ChildActor(out, kafkaActor))
}
