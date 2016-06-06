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
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	//counterExample();
    }
    
    public static void streamingExample (){
    	String appName, master;
    	appName = "org.sparkexample2.streaming";
    	master = "local";
    	
    	
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        //JavaStreamingContext ssc = new JavaStreamingContext(conf, Duration(1000));
        
    }

    
    
    
    
    
    
    public static void counterExample (){
    	//define path
    	String inputFileName = "samples/test.txt" ;
        String outputDirName = "output" ;
        
        //define configuration
        SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> file = context.textFile(inputFileName);
        //mapper
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
		  public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
		  public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
		});
		//reducer
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
		  public Integer call(Integer a, Integer b) { return a + b; }
		});
		counts.saveAsTextFile(outputDirName);
		//Test
    }
    
}


