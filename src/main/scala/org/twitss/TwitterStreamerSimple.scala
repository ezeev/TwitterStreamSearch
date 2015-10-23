package org.twitss

import java.net.{InetAddress, Socket}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.google.gson.Gson
import org.apache.spark.streaming.twitter._
import org.apache.spark.sql.SQLContext
import java.io.{PrintStream, FileWriter}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.io.BufferedSource

object TwitterStreamerSimple {

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

    val Array(sparkMaster, numThreads, consumerKey, consumerSecret, accessToken, accessTokenSecret, metricID) = args.take(7)
    val filters = args.takeRight(args.length - 7)


    // Create the context with a 2 second batch size
    val sparkConf = new SparkConf()
      .setAppName("TwitterStreamer"+metricID)
      .setMaster(sparkMaster + "[" + numThreads + "]")

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
    //val tweetStreamsDF = sqc.read.format("com.databricks.spark.csv").option("header", "true").load(args(6)).cache()
    //we only want the streamTrack_s column
    //val twitterTrackDF = tweetStreamsDF.select("streamTrack_s").toDF()


    //create the twitter stream and map it to JSON format
    val stream = TwitterUtils.createStream(ssc, None, filters).map(gson.toJson(_))
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

        println("THIS IS A TEST")
        println("THERE ARE " + count + " TWEETS WITH THIS INTERVAL")
        this.sendMetric("tweetstreamer.count "+count+" source=tweetstreamer streamId=\""+metricID+"\"")

        println(tweetsDF.show())

      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def sendMetric(metric : String): Unit = {

    val s = new Socket(InetAddress.getByName("localhost"), 2878)
    lazy val in = new BufferedSource(s.getInputStream()).getLines()
    val out = new PrintStream(s.getOutputStream())

    out.println(metric)
    out.flush()
    //println("Received: " + in.next())

    s.close()
    return
  }

}
// scalastyle:on println
