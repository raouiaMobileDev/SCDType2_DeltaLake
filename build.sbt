name := "SCDTypeTwo_Delta"

version := "0.1"

scalaVersion := "2.13.13"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2-M2"

// https://mvnrepository.com/artifact/io.delta/delta-core
libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"


