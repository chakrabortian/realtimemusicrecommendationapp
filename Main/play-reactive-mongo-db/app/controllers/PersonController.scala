package controllers

import javax.inject._

import play.api.Logger
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._
import play.api.mvc._
import play.modules.reactivemongo._
import reactivemongo.api.ReadPreference
import reactivemongo.play.json._
import reactivemongo.play.json.collection.JSONCollection

import scala.concurrent.{ExecutionContext, Future}

/**
  * A bit more complex controller using a Json Coast-to-coast approach. There is no model for Person and some data is created dynamically on creation
  * Input is directly converted to JsObject to be stored in MongoDB
  */
@Singleton
class PersonController @Inject()(val reactiveMongoApi: ReactiveMongoApi)(implicit exec: ExecutionContext) extends Controller with MongoController with ReactiveMongoComponents {

  val transformer: Reads[JsObject] =
    Reads.jsPickBranch[JsString](__ \ "name") and
      Reads.jsPickBranch[JsNumber](__ \ "age") and
      Reads.jsPut(__ \ "created", JsNumber(new java.util.Date().getTime())) reduce

  def personsFuture: Future[JSONCollection] = database.map(_.collection[JSONCollection]("persons"))

  def create(name: String, age: Int) = Action.async {
    val json = Json.obj(
      "name" -> name,
      "age" -> age,
      "created" -> new java.util.Date().getTime())

    for {
      persons <- personsFuture
      lastError <- persons.insert(json)
    } yield Ok("Mongo LastError: %s".format(lastError))

  }

  def createFromJson = Action.async(parse.json) { request =>
    request.body.transform(transformer) match {
      case JsSuccess(person, _) =>
      for {
        persons <- personsFuture
        lastError <- persons.insert(person)
      }
      yield {
        Logger.debug(s"Successfully inserted with LastError: $lastError")
        Created("Created 1 person")
      }
      case _ =>
        Future.successful(BadRequest("invalid json"))
    }
  }

  def createBulkFromJson = Action.async(parse.json) { request =>
    //Transformation silent in case of failures.
    val documents = for {
      persons       <- request.body.asOpt[JsArray].toStream
      maybePerson   <- persons.value
      validPerson   <- maybePerson.transform(transformer).asOpt.toList
    } yield validPerson

    for {
      persons <- personsFuture
      multiResult <- persons.bulkInsert(documents = documents, ordered = true)
    } yield {
      Logger.debug(s"Successfully inserted with multiResult: $multiResult")
      Created(s"Created ${multiResult.n} person")
    }
  }

  def findByName(name: String) = Action.async {
    // let's do our query
    val cursor: Future[List[JsObject]] = personsFuture.flatMap{ persons =>
      // find all people with name `name`
      persons.find(Json.obj("name" -> name)).
      // sort them by creation date
      sort(Json.obj("created" -> -1)).
      // perform the query and get a cursor of JsObject
      cursor[JsObject](ReadPreference.primary).collect[List]()
  }

    // everything's ok! Let's reply with a JsValue
    cursor.map { persons =>
      Ok(Json.toJson(persons))
    }
  }
}
