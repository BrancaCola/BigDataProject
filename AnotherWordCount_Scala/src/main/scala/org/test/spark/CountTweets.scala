package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CountTweets {
  
  def main (args: Array[String]) = {
    
    val conf = new SparkConf()
      .setAppName("CountTweets")
      .setMaster("local")
      
    val sc = new SparkContext(conf)
    
    val tweets = sc.textFile("/home/toni/tweetsHashtagBitcoin518.06._1317_0017.txt")
    
    println(tweets.filter(line => line.contains("Fri Jun 17")).count) //counts Tweets from 17th of June
    
  }
}  
