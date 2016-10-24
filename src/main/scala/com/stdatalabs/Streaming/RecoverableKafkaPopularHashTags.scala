package com.stdatalabs.Streaming

import java.util.HashMap

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

/**
 * A Spark Streaming - Kafka integration to receive twitter data from kafka 
 * topic and find the popular hashtags and also ensure at-least once semantics
 * i.e, zero data loss
 * 
 * Arguments: <zkQuorum> <consumer-group> <topics> <numThreads>
 * <zkQuorum> 					 - The zookeeper hostname
 * <consumer-group>      - The Kafka consumer group
 * <topics>              - The kafka topic to subscribe to
 * <numThreads>          - Number of kafka receivers to run in parallel
 * <checkpointDirectory> - The directory to store and retrieve checkpoint data
 * 
 * More discussion at stdatalabs.blogspot.com
 * 
 * @author Sachin Thirumala
 */

object RecoverableKafkaPopularHashTags {

  def createContext(zkQuorum: String, group: String, topics: String, numThreads: String, checkpointDirectory: String): StreamingContext = {

    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")
    val conf = new SparkConf().setMaster("local[6]").setAppName("Spark Streaming - Kafka Producer - PopularHashTags").set("spark.executor.memory", "1g")

    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // Set the Spark StreamingContext to create a DStream for every 2 seconds  
    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint(checkpointDirectory)

    // Map each topic to a thread  
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // Map value from the kafka message (k, v) pair      
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    // Filter hashtags
    val hashTags = lines.flatMap(_.split(" ")).filter(_.startsWith("#"))

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
    
    if (args.length != 5) {
      System.err.println("Your arguments were " + args.mkString("[", ", ", "]"))
      System.err.println(
        """
          |Usage: RecoverableKafkaPopularHashTags <zkQuorum> <group> <topics>
          |     <numThreads> <checkpointDirectory> 
        """.stripMargin
      )
      System.exit(1)
    }

    // Create an array of arguments: zookeeper hostname/ip,consumer group, topicname, num of threads   
    val Array(zkQuorum, group, topics, numThreads, checkpointDirectory) = args

    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(zkQuorum, group, topics, numThreads, checkpointDirectory))

    ssc.start()
    ssc.awaitTermination()
  }

}