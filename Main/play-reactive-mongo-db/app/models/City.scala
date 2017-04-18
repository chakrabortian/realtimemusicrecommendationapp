package models

import play.api.libs.json.Json

case class City(name: String, population: Int)

object City {
  implicit val formatter = Json.format[City]
}
