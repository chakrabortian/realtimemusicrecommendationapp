package music.recommendation.core

/**
  * Created by deveshkandpal on 4/16/17.
  */
object LyricsTestLogic {

  def main(args : Array[String]) = {

    val s = "love,so,know,this,but,with,what"

    val q = s.split(",").toList.zip (Stream from 1)
    q.foreach(a => println(a))

  }

}
