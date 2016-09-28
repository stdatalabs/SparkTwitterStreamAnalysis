package org.stdatalabs.TwitterPopularHashTags

import org.apache.spark.streaming.{ Seconds, StreamingContext }  
 import org.apache.spark.SparkContext._  
 import org.apache.spark.streaming.twitter._  
 import org.apache.spark.SparkConf  
 import org.apache.spark.streaming._  
 import org.apache.spark.{ SparkContext, SparkConf }  
 import org.apache.spark.storage.StorageLevel  
 import org.apache.spark.streaming.flume._  

 object PopularHashTags {  
  val conf = new SparkConf().setMaster("local[4]").setAppName("PopularHashTags")  
  val sc = new SparkContext(conf)  
  def main(args: Array[String]) {  
   sc.setLogLevel("WARN")  
   System.setProperty("twitter4j.oauth.consumerKey", "ep3OXmXydVoehrDfCZPxh6b7P")  
   System.setProperty("twitter4j.oauth.consumerSecret", "upssXvwVjOoLxfAUYJiM3sF3ydgh7BSsJOBfVE40iJ5h4BcaWx")  
   System.setProperty("twitter4j.oauth.accessToken", "140494769-ppQkBGw1q0jDLFXftKR7hjQXr4oQ9XavTWmZrX6w")  
   System.setProperty("twitter4j.oauth.accessTokenSecret", "nEbsCoqesnOjSxMXyzMlyB4X53eVwOmPBkgVdYwwu6wFx") 

   // Set the Spark StreamingContext to create a DStream for every 5 seconds
   val ssc = new StreamingContext(sc, Seconds(5))  
   // Pass the filter keywords as arguements
   val filter = args.takeRight(args.length)  
   //  val stream = FlumeUtils.createStream(ssc, args(0), args(1).toInt)  
   val stream = TwitterUtils.createStream(ssc,None, filter) 
   // Split the stream on space and extract hashtags 
   val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))  

   // Get the top hashtags over the previous 60 sec window
   val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))                
            .map{case (topic, count) => (count, topic)}  
            .transform(_.sortByKey(false))  

   // Get the top hashtags over the previous 10 sec window
   val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))  
            .map{case (topic, count) => (count, topic)}  
            .transform(_.sortByKey(false)) 

   // print tweets in the currect DStream 
   stream.print()           

   // Print popular hashtags  
   topCounts60.foreachRDD(rdd => {  
    val topList = rdd.take(10)  
    println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))  
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}  
   })  
   topCounts10.foreachRDD(rdd => {  
    val topList = rdd.take(10)  
    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))  
    topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}  
   })  

   ssc.start()  
   ssc.awaitTermination()  
  }  
 }   