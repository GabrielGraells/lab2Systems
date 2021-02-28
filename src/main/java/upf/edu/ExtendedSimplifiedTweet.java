package upf.edu;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.Optional;


public class ExtendedSimplifiedTweet implements Serializable {

    private final long tweetId; // the id of the tweet ('id')
    private final String text; // the content of the tweet ('text')
    private final long userId; // the user id ('user->id')
    private final String userName; // the user name ('user'->'name')
    private final String language; // the language of a tweet ('lang')
    private final Long retweetUserId;
    private final long followersCount;
    private final boolean isRetweeted;
    private final Long retweetedTweedId;
    private final long timestampMs;
    
    

    public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName, String language,Long retweetedUserId, long followersCount, boolean isRetweeted, Long retweetedTweetId, long timestampMs) {

        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.language = language;
        this.timestampMs = timestampMs;
        this.retweetUserId = retweetedUserId;
        this.followersCount = followersCount;
        this.isRetweeted = isRetweeted;
        this.retweetedTweedId = retweetedTweetId;

    }

    /**
     * Returns a {@link SimplifiedTweet} from a JSON String. If parsing fails, for
     * any reason, return an {@link Optional#empty()}
     *
     * @param
     * @return an {@link Optional} of a {@link SimplifiedTweet}
     */
    public long getId() {
        return this.tweetId;
    }

    public String getText() {
        return this.text;
    }

    public long getUserId() {
        return this.userId;
    }

    public String getUserName() {
        return this.userName;
    }

    public String getLanguage() {
        return this.language;
    }

    public long getTimeStampMs() {
        return this.timestampMs;
    }
    
    public long getretweetuid() {
    	return this.retweetUserId;
    }
    
   public Long getfollowersCount() {
	   return this.followersCount;
   }
   
   public boolean isRetweet() {
	   return this.isRetweeted;
   }
   
   public long getretweetid() {
	   return this.retweetedTweedId;
   }
   
   public String getFormatedOutputTweet() {

       String output = "##############################################################\n" +
                       "* TweetID: " + this.tweetId + "\n" +
                       "* UserID: " + this.userId + "\n" +
                       "* UserName: " + this.userName + "\n" +
                       "* Language: " + this.language + "\n"  +
                       "* TimeStamp: " + this.timestampMs + "\n" +
                       "* Text: " + this.text + "\n" +
                       "Retweetid:" + this.retweetedTweedId+"\n"+
                       "Retweetuserid:" + this.retweetUserId+"\n"+
                       "FollowersCount:" + this.followersCount+"\n"+
                       "isretweet" + this.isRetweeted+"\n"+
                       "##############################################################\n\n";

       return output;
   }

    public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {

        Optional<ExtendedSimplifiedTweet> tweetopt = Optional.empty();

        JsonElement je = new JsonParser().parse(jsonStr);

        if (je.isJsonNull())
            return tweetopt;

        JsonObject jo = je.getAsJsonObject();

        long tweetId;
        String text = null;
        long userId = 0;
        String userName = null;
        String language = null;
        long timestampMs;
        Long retweetuid = null;
        boolean isretweeted = false;
        Long retweetid = null;
        long followersCount=0;

        if (jo.has("id") && jo.has("text") && jo.has("user") && jo.has("lang") && jo.has("timestamp_ms")) {
            JsonObject userObject = jo.get("user").getAsJsonObject();

            if (userObject.has("name") && userObject.has("id")) {
                userName = userObject.get("name").getAsString();
                userId = userObject.get("id").getAsLong();
            }
            tweetId = jo.get("id").getAsLong();
            text = jo.get("text").getAsString();
            language = jo.get("lang").getAsString();
            timestampMs = jo.get("timestamp_ms").getAsLong();
            
            
            if(jo.get("retweeted_status")!=null) {
            		JsonObject retweetObject = jo.get("retweeted_status").getAsJsonObject();
            		isretweeted = true;
	            	retweetid = retweetObject.get("id").getAsLong();
	            	JsonObject retweetUser = retweetObject.get("user").getAsJsonObject();
	            	if(retweetUser.has("id")) {
	            		retweetuid = retweetUser.get("id").getAsLong();
	            		followersCount = retweetUser.get("followers_count").getAsLong();
	            	}
	            
            }
            
            ExtendedSimplifiedTweet tweet = new ExtendedSimplifiedTweet(tweetId, text, userId, userName, language,retweetuid, followersCount, isretweeted,retweetid,timestampMs);

            tweetopt = Optional.of(tweet);
        }

        return tweetopt;

    }
}