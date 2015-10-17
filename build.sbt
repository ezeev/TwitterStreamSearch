name := "TwitterMarketCorrelations"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.5.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.1"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1"

libraryDependencies += "com.google.code.gson" % "gson" % "2.4"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.1"

libraryDependencies += "org.scalaj" % "scalaj-http_2.10" % "1.1.5"

libraryDependencies += "org.apache.solr" % "solr-solrj" % "5.3.1"

libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.2.0"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5", "org.slf4j" % "slf4j-simple" % "1.7.5")

fork := true

