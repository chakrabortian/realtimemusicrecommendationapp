package models

import play.api.libs.json.Json

/**
  * Created by deveshkandpal on 4/20/17.
  */

object MongoModels {

  case class UserObj(username : String)
  implicit val UserJsonRead = Json.reads[UserObj]
}
