package spark;


import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.commons.net.ftp.FTP;
import org.apache.spark.SparkConf;
import java.util.Arrays;

public class TwitterFilterLanguageApp {

    public static void main(String[] args){
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);
        String inputDir = argsList.get(3);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("TwitterFilterLanguageApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input

        JavaRDD<String> tweets = sparkContext.textFile(inputDir);
        JavaRDD<SimplifiedTweet> filteredTweets =  tweets
                .map(s-> SimplifiedTweet.fromJson(s))
                .filter(opt -> opt.isPresent())
                .map(tweet -> tweet.get())
                .filter(tweet -> tweet.getLanguage().equals(language));
        /*
        File output = new File(outputFile);
		try {
			output.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        */
        filteredTweets.map(s -> s.getId()+s.getUserId()+s.getUserName()+s.getText()+s.getTimeStampMs()+"##############################################################\\n\\n").saveAsTextFile(outputFile);
        
        
        
		//FileLanguageFilter f = new FileLanguageFilter(output);
        System.out.println("\n\n##############################################################");
        //filteredTweets.map(tweet -> f.filterLanguage(tweet));
        System.out.println(filteredTweets.count());
        System.out.println("##############################################################\n\n");
        
        filteredTweets.saveAsTextFile(outputFile);

        final S3Uploader uploader = new S3Uploader(bucket, language, "default");
        uploader.upload(outputFile);
    }
}
