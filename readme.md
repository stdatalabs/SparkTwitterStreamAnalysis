# SparkTwitterPopularHashTags

A project on Spark Streaming to analyze Popular hashtags from live twitter data streams. Data is ingested from different input sources like Twitter source, Flume and Kafka and processed downstream using Spark Streaming.

## Requirements
- IDE 
- Apache Maven 3.x
- JVM 6 or 7

## General Info
The source folder is organized into 2 packages i.e. Kafka and Streaming. Each class in the Streaming package explores different approach to consume data from Twitter source. Below is the list of classes:
* com/stdatalabs/Kafka
     * KafkaTwitterProducer.java --   A Kafka Producer that publishes twitter data to a kafka broker
* com/stdatalabs/Streaming
    * SparkPopularHashTags.scala -- Receives data from Twitter datasource
    * FlumeSparkPopularHashTags.scala -- Receives data from Flume Twitter producer
    * KafkaSparkPopularHashTags.scala -- Receives data from Kafka Producer
    * RecoverableKafkaPopularHashTags.scala -- Spark-Kafka receiver based approach. Ensures at-least once semantics
    * KafkaDirectPopularHashTags.scala -- Spark-Kafka Direct approach. Ensures exactly once semantics
* TwitterAvroSource.conf 
    -- Flume conf for running Twitter avro source

## Description
* ##### A Spark Streaming application that receives tweets on certain keywords from twitter datasource and finds the popular hashtags. 
  Discussed in blog -- 
     [Spark Streaming part 1: Real time twitter sentiment analysis](http://stdatalabs.blogspot.in/2016/09/spark-streaming-part-1-real-time.html)

* ##### A Spark Streaming - Flume integration to find Popular hashtags from twitter. It receives events from a Flume source that connects to twitter and pushes tweets as avro events to sink.
    Discussed in blog -- 
     [Spark streaming part 2: Real time twitter sentiment analysis using Flume](http://stdatalabs.blogspot.in/2016/09/spark-streaming-part-2-real-time_10.html)
     
* ##### A Spark Streaming - Kafka integration to receive twitter data from kafka producer and find the popular hashtags
    Discussed in blog -- 
     [Spark streaming part 3: Real time twitter sentiment analysis using kafka](http://stdatalabs.blogspot.in/2016/09/spark-streaming-part-3-real-time.html)
     
* ##### A Spark Streaming - Kafka integration to ensure at-least once semantics
    Discussed in blog -- 
     [Data guarantees in Spark Streaming with kafka integration](http://stdatalabs.blogspot.in/2016/10/data-guarantees-in-spark-streaming-with.html)
     
* ##### A Spark Streaming - Kafka integration to ensure exactly once semantics
    Discussed in blog -- 
     [Data guarantees in Spark Streaming with kafka integration](http://stdatalabs.blogspot.in/2016/10/data-guarantees-in-spark-streaming-with.html)



### More articles on hadoop technology stack at [stdatalabs](stdatalabs.blogspot.com)

