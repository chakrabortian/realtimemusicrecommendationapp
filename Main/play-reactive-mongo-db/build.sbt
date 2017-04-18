name := """play-reactive-mongo-db"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

routesGenerator := InjectedRoutesGenerator

libraryDependencies ++= Seq(
  "org.reactivemongo" %% "play2-reactivemongo" % "0.11.12",
  cache,
  ws,
  specs2 % Test

)

libraryDependencies += "com.ning" % "async-http-client" % "1.9.29"

resolvers += "Typesafe private" at
  "https://private-repo.typesafe.com/typesafe/maven-releases"

libraryDependencies +=
  "com.typesafe.play.extras" %% "iteratees-extras" % "1.5.0"


libraryDependencies ++= Seq("org.apache.kafka" % "kafka_2.11" % "0.10.2.0"
exclude("javax.jms", "jms")
exclude("com.sun.jdmk", "jmxtools")
exclude("com.sun.jmx", "jmxri"),
"com.typesafe" % "config" % "1.2.1")

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"


scalacOptions in ThisBuild ++= Seq("-feature", "-language:postfixOps")