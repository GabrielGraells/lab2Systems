package upf.edu;


import java.io.File;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystem;
import java.util.Arrays;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
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
        String outputDir = argsList.get(1);
        String inputDir = argsList.get(2);

        String outputDirTask = outputDir + "/" + language;
        String outputDirBenchmark = "output/benchmark/" + "(" + language +")" +"benchmark.txt";

        // Start clock
        long startTime = System.nanoTime();

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

        filteredTweets.map(s -> s.getFormatedOutputTweet()).saveAsTextFile(outputDirTask);

        //End clock - Time elapsed
        long endTime = System.nanoTime();
        long totalTime = endTime - startTime;

        //Benchmark format and uploading
        String benchmarkOutput = "Language: " + language + "\nTotal number of tweets: " + filteredTweets.count() + "\nTotal running time in nanoseconds: " +  totalTime + " ns\n\n";

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        InputStream outputStream = new ByteArrayInputStream(benchmarkOutput.getBytes(StandardCharsets.UTF_8));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("plain/text");
        final byte[] lengthByte;
        try {
            lengthByte = benchmarkOutput.getBytes("UTF-8");
            metadata.setContentLength(lengthByte.length);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        PutObjectRequest request = new PutObjectRequest("edu.upf.ldsd2021.lab2.grp101.team03",outputDirBenchmark , outputStream, metadata);
        s3Client.putObject(request);

    }
}
