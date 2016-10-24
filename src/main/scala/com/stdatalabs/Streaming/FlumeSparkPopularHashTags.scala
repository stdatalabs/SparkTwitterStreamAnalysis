package com.stdatalabs.Streaming

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume._

/**
 * A Spark Streaming - Flume integration to find Popular hashtags from twitter
 * It receives events from a Flume source that connects to twitter and pushes 
 * tweets as avro events to sink.
 * 
 * More discussion at stdatalabs.blogspot.com
 * 
 * @author Sachin Thirumala
 */

object FlumeSparkPopularHashTags {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark Streaming - Flume Source - PopularHashTags")
  val sc = new SparkContext(conf)
  def main(args: Array[String]) {
    sc.setLogLevel("WARN")
    // Set the Spark StreamingContext to create a DStream for every 5 seconds  
    val ssc = new StreamingContext(sc, Seconds(5))
    val filter = args.takeRight(args.length)

    // Create stream using FlumeUtils to receive data from flume at hostname: <hostname> and port: <port>  
    val stream = FlumeUtils.createStream(ssc, "ubuntu", 9988)
    val tweets = stream.map(e => new String(e.event.getBody.array))
    tweets.print()

    // Split the stream on space and extract hashtags   
    val hashTags = tweets.flatMap(status => status.split(" ").filter(_.startsWith("#")))
    // Get the top hashtags over the previous 60 sec window  
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Get the top hashtags over the previous 10 sec window  
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Print popular hashtags  
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    stream.count().map(cnt => "Received " + cnt + " flume events.").print()
    ssc.start()
    ssc.awaitTermination()
  }
}  