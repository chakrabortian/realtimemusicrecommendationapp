package controllers

import javax.inject._

import actors.KafkaActor.ExistingUserRecommendation
import models.MongoModels._
import actors._
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.Materializer
import org.apache.commons.lang3.StringEscapeUtils
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
import scala.concurrent.duration._

/**
  * Created by deveshkandpal on 4/17/17.
  */


@Singleton()
class UserController @Inject()(val reactiveMongoApi: ReactiveMongoApi)(implicit exec: ExecutionContext, implicit val mat: Materializer) extends Controller with MongoController with ReactiveMongoComponents {

  val system =  ActorSystem("reactiveappactorsystem")


  val kafkaActor : ActorRef = system.actorOf(Props[KafkaActor], name = "KafkaActor")

  system.scheduler.schedule(5000 milliseconds, 5000 milliseconds, kafkaActor, KafkaActor.Subscribe )

  def getUsersCollection : Future[JSONCollection] = database.map(_.collection[JSONCollection]("users"))
  def getArtistCollection : Future[JSONCollection] = database.map(_.collection[JSONCollection]("userartist"))
  def getSongCollection : Future[JSONCollection] = database.map(_.collection[JSONCollection]("artistsong"))


  def getUserArtist(userId : String) = Action.async { request =>
    val cursor : Future[List[JsObject]] = getArtistCollection.flatMap {
      u =>
        u.find(Json.obj("user" -> userId)).cursor[JsObject](ReadPreference.primary).collect[List]()
    }
    cursor.map(c => {
      val user = (c(0) \ "user").as[String]
      val tempArt = (c(0) \ "artist").as[String]
      val artist = StringEscapeUtils.unescapeXml(tempArt)

      val json = Json.obj(
        "user" -> user,
        "artist" -> artist
      )
      Ok(json)

    })
  }

  def getArtistSongs(artistName : String) = Action.async {

    request =>
      val cursor : Future[List[JsObject]] = getSongCollection.flatMap {
        u =>
          u.find(Json.obj("artist" -> artistName)).cursor[JsObject](ReadPreference.primary).collect[List]()
      }
      cursor.map(c => {
        val artist = (c(0) \ "artist").as[String]
        val tempSongs = (c(0) \ "songs").as[String]
        val songs = StringEscapeUtils.unescapeXml(tempSongs)

        val json = Json.obj(
          "artist" -> artist,
          "songs" -> songs
        )
        Ok(json)
      })
  }

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
        Created(json)
      }
    })

  }

//  def signInUser = Action.async(parse.json) { request =>
//    println("User request received")
//    val bodyAsJsValue = request.body
//    val userName = (bodyAsJsValue \ "userName").as[String]
//    val password = (bodyAsJsValue \ "password").as[String]
//4
//
//    val cursor: Future[List[JsObject]] = getUsersCollection.flatMap{ u =>
//      u.find(Json.obj("userName" -> userName, "password" -> password)).cursor[JsObject](ReadPreference.primary).collect[List]()
//    }
//
//    cursor.map(c => {
//      if (c.size == 1) {
//        println("User found !!")
//
//
//      val userName = (c(0) \ "userName").as[String]
//      val password = (c(0) \ "password").as[String]
//
//      val json = Json.obj(
//        "userName" -> userName,
//        "password" -> password
//      )
//      Ok(json)
//    }
//      else {
//        println("No User found !!")
//        BadRequest("No User found")
//      }
//
//    })
//  }

  def signInUser = Action.async(parse.json) { request =>
    println("User request received")
    val bodyAsJsValue = request.body
    val userName = (bodyAsJsValue \ "userName").as[String]
    val password = (bodyAsJsValue \ "password").as[String]


    val cursor: Future[List[JsObject]] = getUsersCollection.flatMap{ u =>
      u.find(Json.obj("userName" -> userName, "password" -> password)).cursor[JsObject](ReadPreference.primary).collect[List]()
    }

    cursor.map(c => {
      if (c.size == 1) {
        println("User found !!")


        val userName = (c(0) \ "userName").as[String]
        val password = (c(0) \ "password").as[String]

        val json = Json.obj(
          "userName" -> userName,
          "password" -> password
        )
        Ok(json)
      }
      else {
        println("No User found !!")
        BadRequest("No User found")
      }

    })
  }


  def acceptUserSurvey = WebSocket.acceptWithActor[String,String] {
    request => out => ColdStartActor.props(out, kafkaActor)
  }

  def similarArtist = WebSocket.acceptWithActor[String,String] {
    request => out => ArtistForArtistActor.props(out, kafkaActor)
  }

  def lyricsBasedReco = WebSocket.acceptWithActor[String,String] {
    request => out => SongLyricsActor.props(out, kafkaActor)
  }



  def existingUserReco = WebSocket.acceptWithActor[String,String] {
    request => out => ExistingUserActor.props(out, kafkaActor)
  }

}


