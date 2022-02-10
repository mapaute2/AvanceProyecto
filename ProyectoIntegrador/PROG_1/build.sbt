name := "ProyectoIntegrador"

version := "0.1"

scalaVersion := "2.13.7"

// Core library, included automatically if any other module is imported.
libraryDependencies += "com.nrinaudo" %% "kantan.csv" % "0.6.2"
libraryDependencies += "com.nrinaudo" %% "kantan.csv-generic" % "0.6.2"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.2"
libraryDependencies += "com.typesafe.slick" %% "slick" % "3.3.3"

libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.33"
libraryDependencies += "com.oracle.database.jdbc" % "ojdbc11" % "21.4.0.0.1"
libraryDependencies += "com.typesafe.slick" %% "slick" % "3.3.3"
