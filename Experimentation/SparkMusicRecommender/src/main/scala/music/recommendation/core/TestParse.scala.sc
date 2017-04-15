import scala.io.Source

val fileName = "C:\\Users\\chakr\\Desktop\\test.txt"

//val fileContents = Source.fromFile(filename).getLines.mkString

val fileContents = Source.fromFile(fileName).getLines().map(line => {
  line.split("\\W")
})

//for(line <- fileContents) println(line.toList)

val trainData = fileContents.map(p => CC1(p(0), p(1), p(2)))

for(x <- trainData) println(x)


case class CC1(userId: String, songId: String, playCount: String)
