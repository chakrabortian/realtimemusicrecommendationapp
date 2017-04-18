package controllers

import javax.inject._

import actors.{ChildActor, KafkaActor}
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.Materializer
import play.api.Logger
import play.api.libs.json.Reads._
import play.api.libs.json._
import play.modules.reactivemongo._
import reactivemongo.api.ReadPreference
import reactivemongo.play.json._
import reactivemongo.play.json.collection.JSONCollection
import play.api.libs.functional.syntax._
import play.api.Play.current
import play.api._
import play.api.mvc._

// import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.{JsObject, JsValue}
import play.api.libs.ws.WS
import play.extras.iteratees.{Encoding, JsonIteratees}
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by deveshkandpal on 4/17/17.
  */


@Singleton()
class UserController @Inject()(val reactiveMongoApi: ReactiveMongoApi)(implicit exec: ExecutionContext, implicit val mat: Materializer) extends Controller with MongoController with ReactiveMongoComponents {

  val system =  ActorSystem("reactiveappactorsystem")
  val kafkaActor : ActorRef = system.actorOf(Props[KafkaActor], name = "KafkaActor")

  def getUsersCollection : Future[JSONCollection] = database.map(_.collection[JSONCollection]("users"))

  def createUser = Action.async(parse.json) { request =>


    val bodyAsJsValue = request.body
    val userName = (bodyAsJsValue \ "userName").as[String]
    val password = (bodyAsJsValue \ "password").as[String]



    val cursor: Future[List[JsObject]] = getUsersCollection.flatMap{ u =>

      u.find(Json.obj("userName" -> userName)).cursor[JsObject](ReadPreference.primary).collect[List]()
    }

     cursor.map(c => {
      if(c.size >= 1)
        BadRequest("User Already Exist!")
      else {

        val json = Json.obj(
          "userName" -> userName,
          "password" -> password
        )

        for {
          users <- getUsersCollection
          lastError <- users.insert(json)
        }
          yield {
            Logger.info(s"Successfully inserted with LastError: $lastError")

          }
        Created("Created 1 User")
      }
    })

  }

  def signInUser = Action.async(parse.json) { request =>

    val bodyAsJsValue = request.body
    val userName = (bodyAsJsValue \ "userName").as[String]
    val password = (bodyAsJsValue \ "password").as[String]


    val cursor: Future[List[JsObject]] = getUsersCollection.flatMap{ u =>

      u.find(Json.obj("userName" -> userName, "password" -> password)).cursor[JsObject](ReadPreference.primary).collect[List]()
    }

    cursor.map(c => {
      if(c.size == 1)
        Ok(c(0))
      else
        BadRequest("No User found")
    })

  }


  def acceptUserSurvey = WebSocket.acceptWithActor[String,String] {
    request => out => ChildActor.props(out, kafkaActor)
  }



}


