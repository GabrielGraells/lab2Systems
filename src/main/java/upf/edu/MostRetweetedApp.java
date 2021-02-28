package upf.edu;




import java.util.Arrays;
import java.util.Comparator;



import scala.Tuple2;

import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.commons.net.ftp.FTP;
import org.apache.spark.SparkConf;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;

public class MostRetweetedApp {

    private static JavaSparkContext jsc;

	public static void main(String[] args){
        List<String> argsList = Arrays.asList(args);

        String outputFile = argsList.get(1);
        String inputDir = argsList.get(0);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("MostRetweetedApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input
        JavaRDD<String> tweets = sparkContext.textFile(inputDir);
        
        //original_tweets
        JavaRDD<ExtendedSimplifiedTweet> original_tweets =  tweets
                .map(s-> ExtendedSimplifiedTweet.fromJson(s))
                .filter(opt -> opt.isPresent())
                .map(tweet -> tweet.get())
                ; 
        
        //retweeted_tweets
        JavaRDD<ExtendedSimplifiedTweet> filteredTweets =  original_tweets
                .filter(opt -> opt.isRetweet()==true)
                ; 
        
        //***Get most retweeted users***
        JavaPairRDD<Long, Integer> users = filteredTweets.mapToPair(s -> new Tuple2<Long,Integer>(s.getretweetuid(),1))
        		.reduceByKey((x, y) -> x + y);
        
        JavaPairRDD<Tuple2<Integer,Long>, Integer> countInKey = users.mapToPair(a -> new Tuple2(new Tuple2<Integer,Long>(a._2, a._1), null));
        
        // sort by num of occurences
        JavaPairRDD<Tuple2<Integer, Long>, Integer> retweetusersSortedByCount = countInKey.sortByKey(new TupleComparator(),false);

        // Take first 10   
        List<Tuple2<Tuple2<Integer, Long>, Integer>> output = retweetusersSortedByCount.take(3);
      
        //Create a list with the top 10
        List<Long> users_2= new ArrayList<>();
        output.forEach(s->users_2.add(s._1._2));

        //***Get most retweeted tweets***
        
        JavaPairRDD<Long, Integer> retweets = filteredTweets.mapToPair(s -> new Tuple2<Long,Integer>(s.getretweetid(),1))
        		.reduceByKey((x, y) -> x + y);
        
        JavaPairRDD<Tuple2<Integer,Long>, Integer> retweetInKey = users.mapToPair(a -> new Tuple2(new Tuple2<Integer,Long>(a._2, a._1), null));
        
        //sort
        JavaPairRDD<Tuple2<Integer, Long>, Integer> retweetSortedByCount = countInKey.sortByKey(new TupleComparator(),false);

        // collect result    
        List<Tuple2<Tuple2<Integer, Long>, Integer>> outputr = retweetSortedByCount.collect();
      
        //Add the retweetids to a list, to later filter
        List<Long> retweets_3 = new ArrayList<>();
        outputr.forEach(s->retweets_3.add(s._1._2));
        
        //**Keep only the tweets that were retweeted**
        JavaRDD <ExtendedSimplifiedTweet> retweets_rdd = filteredTweets.filter(s->retweets_3.contains(s.getId()));
        

        retweets_rdd.take(5).forEach(s->System.out.println(s.getId()+"###########"));
        
        /*
        //Print most retweeted users and retweets
        for (Tuple2<?, ?> tuple : output) {
        	System.out.println("##############################################");
            System.out.println(tuple._1());
        }
        
        for (Tuple2<?, ?> tuple : outputr) {
        	System.out.println("##############################################");
            System.out.println(tuple._1());
            //System.out.println(retweets_3.isEmpty());
        }
      */
        
      
        //Print retweeted tweets
        for (Long usrs: users_2) {
	        	JavaRDD<ExtendedSimplifiedTweet>  fin = retweets_rdd.filter(s->s.getUserId()==usrs);
	        	List<ExtendedSimplifiedTweet> test = fin.take(3);
	        	
	        	for (ExtendedSimplifiedTweet t: test) {
	        		System.out.println("##############################################");
	                System.out.println(t.getFormatedOutputTweet()+"#######");
	        	}
	        	
	        	
        	}
        	//JavaRDD<ExtendedSimplifiedTweet> rdd =  sparkContext.parallelize(test);	
        	
        	//rdd.map(s->s.getFormatedOutputTweet()).repartition(1).saveAsTextFile(outputFile+usrs);	
        	
        	
        	
        
    }
}
