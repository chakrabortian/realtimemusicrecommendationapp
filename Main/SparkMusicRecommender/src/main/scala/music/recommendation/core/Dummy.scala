package music.recommendation.core

import music.recommendation.bo.UserSongLyricsPosInfo

/**
  * Created by deveshkandpal on 4/17/17.
  */
object Dummy {

  def main(args : Array[String]) : Unit = {

    val a = UserSongLyricsPosInfo("devesh", "1", "1", 1, 10, "Adj", 10)
    val b = UserSongLyricsPosInfo("devesh", "1", "1", 1, 9, "Verb", 11)
    val c = UserSongLyricsPosInfo("devesh", "2", "1", 1, 9, "Verb", 9)
    val d = UserSongLyricsPosInfo("devesh", "2", "1", 1, 10, "Adj", 12)
    val e = UserSongLyricsPosInfo("devesh", "3", "1", 1, 6, "Interjection", 1)

    val f = UserSongLyricsPosInfo("priyesh", "4", "1", 1, 1, "Adj", 1)
    val g = UserSongLyricsPosInfo("priyesh", "5", "1", 1, 2, "Adj", 10)
    val h = UserSongLyricsPosInfo("priyesh", "6", "1", 1, 3, "Verb", 9)
    val i = UserSongLyricsPosInfo("priyesh", "7", "1", 1, 4, "Verb", 2)
    val j = UserSongLyricsPosInfo("priyesh", "8", "1", 1, 5, "Verb", 12)


    val l = List(a,b,c,d,e,f,g,h,i,j)

    val q = l.groupBy(us => us.userId).flatMap(us1 => us1._2.groupBy(us2 => us2.pos).map(us3 => us3._2.sortWith((a, b) => a.wordCount > b.wordCount).take(1))).flatMap(a => a).groupBy(a => a.userId).filter(p => p._2.size == 2).flatMap(a => a._2)




//      val p = q.groupBy(us2 => us2.pos)
//      .map(us3 => us3._2.toList.sortWith((p1, p2) => p1.wordCount > p2.wordCount))
//      .flatMap(a => a)

    q.toList.foreach(a => println(a))
    println("aaaaaaaa")




  }

}
