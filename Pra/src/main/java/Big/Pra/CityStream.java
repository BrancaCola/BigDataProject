package Big.Pra;

public class CityStream {

	public static void main(String[] args){
		  boolean log4jInitialized=Logger.getRootLogger().getAllAppenders().hasMoreElements();
		  Logger.getLogger(CityStream.class).info("Setting log level to [WARN] for streaming example." + " To override add a custom log4j.properties to the classpath.");
		  Logger.getRootLogger().setLevel(Level.WARN);
		  System.setProperty("twitter4j.oauth.consumerKey","hGFBaAJKJiK1uh99uyZ2pdNpf");
		  System.setProperty("twitter4j.oauth.consumerSecret","rvK7pj1pS4Y68Y2XYkdMU3m8sgbjyqIzJOdzkzeXLCmaX2YSFC");
		  System.setProperty("twitter4j.oauth.accessToken","84158599-g2VgFmSKGr7ZpVKiK055kiwK4xhwPb1CfGpXWgE2c");
		  System.setProperty("twitter4j.oauth.accessTokenSecret","cqqECq8kSoJk3WKyrCJCohR5s5YJL0AqVlstgWa4zUlJQ");
		  SparkConf conf=new SparkConf().setMaster("local[3]").setAppName("BusProcessor");
		  JavaStreamingContext sc=new JavaStreamingContext(conf,new Duration(10000));
		  String[] query=new String[4];
		  query[0]="So Paulo";
		  query[1]="teste";
		  query[2]="maria";
		  query[3]="Blatter";
		  JavaDStream<Status> tweets=TwitterUtils.createStream(sc,null,query);
		  JavaDStream<String> x=tweets.map(s -> s.getText());
		  JavaPairDStream<String,String> t=tweets.mapToPair(s -> new Tuple2<String,String>(s.getUser().getName(),s.getText()));
		  t.saveAsHadoopFiles("c:/teste.txt","txt");
		  t.print();
		  sc.start();
		  sc.awaitTermination();
		}
		 
}
