package upf.edu;




import java.util.Arrays;
import java.util.Comparator;



import scala.Tuple2;
import scala.Tuple3;

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
        
        long startTime = System.nanoTime();

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
      
        
        //***Keep users with more than 10 tweets retweeted***
        //JavaPairRDD<Tuple2<Integer, Long>, Integer> retweetusersSortedByCountmore10 = retweetusersSortedByCount.filter(m->filteredTweets.filter(s->s.getretweetuid()==m._1._2).distinct().count()>10);
        

        //***Get most retweeted tweets***
      
        JavaPairRDD<Long, Integer> retweets = filteredTweets.mapToPair(s -> new Tuple2<Long,Integer>(s.getretweetid(),1))
        		.reduceByKey((x, y) -> x + y);
        
        JavaPairRDD<Tuple2<Integer,Long>, Integer> retweetInKey = retweets.mapToPair(a -> new Tuple2(new Tuple2<Integer,Long>(a._2, a._1), null));
        
        //sort
        JavaPairRDD<Tuple2<Integer,Long>, Integer> retweetSortedByCount = retweetInKey.sortByKey(new TupleComparator(),false);
        
        //We tried many ways using mapToPair(s -> new Tuple2<Long, ExtendedSimplifiedTweet> and .mapToPair(s->new Tuple2<>(new Tuple2<>(s.getretweetid(),s.getretweetuid()),s)
        //However, we couldn't managed to get extendedsimplifiedtweet for each retweet, to print the output
        
        //Add the retweetids to a list, to later filter with order. We know this is not optimal, we tried different options with rdds but didn't compile
        List<Long> retweets_3 = retweetSortedByCount.map(x->x._1._2).collect(); 
    
        //***Used for debugging***
        /*
        //Print most retweeted users and retweets
        for (Tuple2<?, ?> tuple : output) {
        	System.out.println("##############################################-USERS");
            System.out.println(tuple._1());
        }*/
        
        /* for (Tuple2<?, ?> tuple : outputr) {
        	System.out.println("##############################################-RETWEETS");
            System.out.println(tuple._1());
        }
        */
        
        //Print console most retweeted tweets for most retweeted users
        retweetusersSortedByCount.take(10).forEach(s->original_tweets.filter(p->p.getUserId()==s._1._2).filter(m->retweets_3.contains(m.getId())).take(10).forEach(t->System.out.println(t.getFormatedOutputTweet())));
        
        /*
        //Print retweeted tweets using parallelize list	
	        /*JavaRDD<ExtendedSimplifiedTweet>  fin = original_tweets.filter(s->s.getUserId()==usrs).filter(s->retweets_3.contains(s.getId()));
	        List<ExtendedSimplifiedTweet> test = fin.take(10);
	        	
	        JavaRDD<ExtendedSimplifiedTweet> rdd =  sparkContext.parallelize(test);	
	        	
	        rdd.map(s->s.getFormatedOutputTweet()).repartition(1).saveAsTextFile(outputFile+usrs);	
        	
       */
        
        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        System.out.println("Time elapsed:"+timeElapsed/1000000+" miliseconds");
         
    }
   
        	
}
