package org.twitss

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.google.gson.Gson
import org.apache.spark.streaming.twitter._
import org.apache.spark.sql.SQLContext
import java.io.FileWriter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object TwitterStreamer {

  private var gson = new Gson()
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var numTweetsThisInterval = 0
  private var twitterTrack = ArrayBuffer[String]()

  private val logger = LoggerFactory.getLogger("TwitterStreamer")


  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage: TwitterStreamer <sparkMaster> <numThreads> <consumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <pathToTwitterTracks>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(sparkMaster, numThreads, consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(6)

    // Create the context with a 2 second batch size
    val sparkConf = new SparkConf()
      .setAppName("TwitterStreamer")
      .setMaster(sparkMaster+"["+numThreads+"]")

    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //Twitter OAuth Properties
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //create a spark context so we can load twitterstreams.csv
    //we'll use the streamTrack_s column to build the track param that gets
    //sent to the twitter API.
    //See https://dev.twitter.com/streaming/overview/request-parameters#track
    val sc = SparkContext.getOrCreate(sparkConf)
    val sqc = SQLContext.getOrCreate(sc)
    import sqc.implicits._
    //load the csv directly to a Spark DataFrame and cache it
    val tweetStreamsDF = sqc.read.format("com.databricks.spark.csv").option("header", "true").load(args(6)).cache()
    //we only want the streamTrack_s column
    val twitterTrackDF = tweetStreamsDF.select("streamTrack_s").toDF()

    twitterTrack.clear()

    //iterate through the rows to build an array of tracks we want to send to twitter.
    twitterTrackDF.map(t => t(0).toString).collect().foreach(buildTrackStr)

    def buildTrackStr(track : String) {
      twitterTrack += track
    }

    //create the twitter stream and map it to JSON
    val stream = TwitterUtils.createStream(ssc, None, twitterTrack).map(gson.toJson(_))
    //val stream = TwitterUtils.createStream(ssc, None).map(gson.toJson(_))


    val partitionsEachInterval = 2

    stream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(partitionsEachInterval)
        //in case you want to store the raw output
        /*outputRDD.saveAsTextFile(
          outputDirectory + "/tweets_" + time.milliseconds.toString)*/
        numTweetsCollected += count

        //read the tweets into a DataFrame
        val tweetsDF = sqc.read.json(outputRDD)
        tweetsDF.registerTempTable("tweets")
        //get the text column from the tweets
        val tweetsDF2 = sqc.sql("select text from tweets")


        //create data structures to hold the counts for each track, and the sum of scores.
        val tweetScores = mutable.HashMap.empty[String, Double]
        val tweetCounts = mutable.HashMap.empty[String,Int]


        println(tweetsDF.show())

        //iterate throgh the tweet text
        tweetsDF2.collect.foreach(collectTweetMetrics)
        tweetsDF2.write.json("tweets/"+time.milliseconds.toString+".json")



        try {
          val fw = new FileWriter("temp.txt", true)

          //the collectTweetMetrics populated hashmaps with counts and sum of scores for all
          //symbols that matched tweets from this batch. Compute the average score for this batch.
          tweetCounts.foreach {
            case (k,v) =>
              val avgScore = tweetScores(k) / tweetCounts(k)
              fw.write(
                "\n" + time.toString + " - Tweets for " + k
                  + " - " + v + " - Average Score: " + avgScore.toString
              )
              logger.info(s"$k has $v tweets. Average score: $avgScore")
          }
          fw.close()
        }

        //reset
        tweetCounts.clear()
        tweetScores.clear()

        def collectTweetMetrics(tweet: org.apache.spark.sql.Row): Unit = {

          val tweetText = tweet(0).toString

          //val fw = new FileWriter("temp.txt", true)
          //fw.write("TWEET:"+ tweetText+"\n")


          //Send the tweet text to Solr so we can get a relevancy score and see if the tweets match our corpus of tracks we want to track.
          val url = "http://localhost:8983/solr/tweettracks/select?fl=id,score&defType=edismax&wt=csv&qf=relevantTerms_t&q=" + java.net.URLEncoder.encode(tweetText, "utf-8")
          val result = scala.io.Source.fromURL(url).mkString

          //the response is a CSV
          val rows = result.split("\n")
          if (rows.size > 1) {
            try {

              for (i <- 1 to rows.size-1) {

                val cols = rows(i).split(",")

                val symbol = cols(0)
                val relScore = cols(1).toDouble

                //update the counts and add the score to our 2 hashmaps
                if(!tweetScores.contains(symbol)) {
                  tweetCounts += (symbol -> 1)
                  tweetScores += (symbol -> relScore)
                } else {
                  tweetCounts(symbol) = tweetCounts(symbol).+(1)
                  tweetScores(symbol) = tweetScores(symbol).+(relScore)
                }
                println(tweetScores.toString)

              }

            }
          }
          //fw.close()
        }


      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
