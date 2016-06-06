package Big.Pra;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

/*
 * install (OpenBSD)-Netcat
 * in console: nc -lk 9999
 * run JavaNetworkWordCount.java
 * type in stuff
 */

public class JavaNetworkWordCount {
    
    public static void streamingExample () {
    	
    	SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
    	JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        
    	 // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        
        // Split each line into words
        JavaDStream<String> words = lines.flatMap(
          new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) {
              return Arrays.asList(x.split(" "));
            }
          });
        
     // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
          new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
              return new Tuple2<String, Integer>(s, 1);
            }
          });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
          new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
              return i1 + i2;
            }
          });

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();
    	
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
        
    }
    
	public static void main( String[] args ) {
    	
		streamingExample();
		
    }
}