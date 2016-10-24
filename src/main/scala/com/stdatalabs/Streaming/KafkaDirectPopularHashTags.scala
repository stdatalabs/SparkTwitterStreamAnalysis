package com.stdatalabs.Streaming

import java.util.HashMap

import kafka.serializer.StringDecoder

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.storage.StorageLevel
import _root_.kafka.serializer.StringDecoder

/**
 * A Spark Streaming - Kafka integration to receive twitter data from 
 * kafka topic and find the popular hashtags and also ensure exactly once semantics
 * i.e, process data only once
 * 
 * Arguments: <brokers> <topics> <checkpointDir>
 * <brokers>             -	List of one or more Kafka brokers
 * <topics>              -	List of one or more kafka topics to consume from
 * <checkpointDirectory> -  The directory to store and retrieve checkpoint data 
 * 
 * More discussion at stdatalabs.blogspot.com
 * 
 * @author Sachin Thirumala
 */

object KafkaDirectReciverPopularHashTags {

  def createContext(brokers: String, topics: String, checkpointDirectory: String): StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")

    val conf = new SparkConf().setMaster("local[6]").setAppName("Spark Streaming - Kafka DirectReceiver - PopularHashTags").set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // Set the Spark StreamingContext to create a DStream for every 2 seconds  
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    // Define the Kafka parameters, broker list must be specified
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      // start from the smallest available offset, ie the beginning of the kafka log
      "auto.offset.reset" -> "largest")

    // Define which topics to read from
    val topicsSet = topics.split(",").toSet

    // Map value from the kafka message (k, v) pair      
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    // Filter hashtags
    val hashTags = lines.map(_._2).flatMap(_.split(" ")).filter(_.startsWith("#"))

    // Get the top hashtags over the previous 60/10 sec window   
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    lines.print()

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

    lines.count().map(cnt => "Received " + cnt + " kafka messages.").print()

    ssc

  }

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println(s"""
        |Usage: KafkaDirectPopularHashTags <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |  <checkpointDirectory> the directory where the metadata is stored
        |
        """.stripMargin)
      System.exit(1)
    }

    // Create an array of arguments: brokers, topicname, checkpoint directory  
    val Array(brokers, topics, checkpointDirectory) = args

    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(brokers, topics, checkpointDirectory))

    ssc.start()
    ssc.awaitTermination()
  }

}