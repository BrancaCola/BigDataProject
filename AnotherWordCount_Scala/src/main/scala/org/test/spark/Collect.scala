package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter._
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils

object Collect {
  def main(args: Array[String]) {
    
    if (args.length < 4) {
      System.err.println("Usage: TwitterTags <consumer key> <consumer secret> " + "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }
    
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)
    
    //Set the system properties so that Twitter4j library used by twitter stream
    //can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    
    val sparkConf = new SparkConf().setAppName("TwitterTags").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3600))
    val stream = TwitterUtils.createStream(ssc, None, filters)
    
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.contains("bitcoin")))
    //val hashTags = stream.flatMap(status => status.getTags(status).filter(_.startsWith("#bitcoin")))
    //val text = stream.flatMap(status => status.getText.split(" "))
    val text = stream.map(status => status.getText()).filter(_.contains("bitcoin"))
    
    val topCounts300 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(3600))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))
//    val texts        = text.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(300))
    
//    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
//                     .map{case (topic, count) => (count, topic)}
//                     .transform(_.sortByKey(false))
//                     
//    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
//                     .map{case (topic, count) => (count, topic)}
//                     .transform(_.sortByKey(false))
                     
                     
                     
                     
    //Print bitcoin hashtags
    
      topCounts300.foreachRDD(rdd => {
        val topList = rdd.take(3600)
        println("\nTweets about \"bitcoin\" in the last 60 minutes (%s total):".format(rdd.count()))
        topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
      })               
      
//      topCounts60.foreachRDD(rdd => {
//      val topList = rdd.take(10)
//      println("\nPopular topics in the last 60 seconds (%s total):".format(rdd.count()))
//      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
//    })
    
//    topCounts10.foreachRDD(rdd => {
//      val topList = rdd.take(10)
//      println("\nPopular topics in the last 10 seconds (%s total):".format(rdd.count()))
//      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
//    })
    
    topCounts300.saveAsTextFiles("/home/toni/test/TwitterData/bitcoin/hashtags/bitcoinTweets")
    //text.saveAsTextFiles("/home/toni/test/TwitterData/bitcoin/texts/bitcoinTweetsText")
    
    
    ssc.start()
    ssc.awaitTermination()
  }
}