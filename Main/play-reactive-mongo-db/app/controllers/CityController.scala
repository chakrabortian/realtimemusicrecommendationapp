package controllers

import javax.inject._

import models.City
import play.api.Logger
import play.api.libs.json._
import play.api.mvc._
import play.modules.reactivemongo._
import reactivemongo.api.ReadPreference
import reactivemongo.play.json._
import reactivemongo.play.json.collection._
import utils.Errors

import scala.concurrent.{ExecutionContext, Future}


/**
  * Simple controller that directly stores and retrieves [models.City] instances into a MongoDB Collection
  * Input is first converted into a city and then the city is converted to JsObject to be stored in MongoDB
  */
@Singleton
class CityController @Inject()(val reactiveMongoApi: ReactiveMongoApi)(implicit exec: ExecutionContext) extends Controller with MongoController with ReactiveMongoComponents {

  def citiesFuture: Future[JSONCollection] = database.map(_.collection[JSONCollection]("city"))

  def create(name: String, population: Int) = Action.async {
    for {
      cities <- citiesFuture
      lastError <- cities.insert(City(name, population))
    } yield
      Ok("Mongo LastError: %s".format(lastError))
  }

  def createFromJson = Action.async(parse.json) { request =>
    Json.fromJson[City](request.body) match {
      case JsSuccess(city, _) =>
        for {
          cities <- citiesFuture
          lastError <- cities.insert(city)
        } yield {
          Logger.debug(s"Successfully inserted with LastError: $lastError")
          Created("Created 1 city")
        }
      case JsError(errors) =>
        Future.successful(BadRequest("Could not build a city from the json provided. " + Errors.show(errors)))
    }
  }

  def createBulkFromJson = Action.async(parse.json) { request =>
    Json.fromJson[Seq[City]](request.body) match {
      case JsSuccess(newCities, _) =>
        citiesFuture.flatMap { cities =>
          val documents = newCities.map(implicitly[cities.ImplicitlyDocumentProducer](_))

          cities.bulkInsert(ordered = true)(documents: _*).map { multiResult =>
            Logger.debug(s"Successfully inserted with multiResult: $multiResult")
            Created(s"Created ${multiResult.n} cities")
          }
        }
      case JsError(errors) =>
        Future.successful(BadRequest("Could not build a city from the json provided. " + Errors.show(errors)))
    }
  }

  def findByName(name: String) = Action.async {
    // let's do our query
    val futureCitiesList: Future[List[City]] = citiesFuture.flatMap {
      // find all cities with name `name`
      _.find(Json.obj("name" -> name)).
      // perform the query and get a cursor of JsObject
      cursor[City](ReadPreference.primary).
      // Coollect the results as a list
      collect[List]()
    }

    // everything's ok! Let's reply with a JsValue
    futureCitiesList.map { cities =>
      Ok(Json.toJson(cities))
    }
  }
}


