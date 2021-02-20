package spark;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Optional;

public class SimplifiedTweet {

    private final long tweetId; // the id of the tweet ('id')
    private final String text; // the content of the tweet ('text')
    private final long userId; // the user id ('user->id')
    private final String userName; // the user name ('user'->'name')
    private final String language; // the language of a tweet ('lang')
    private final long timestampMs; // seconds from epoch ('timestamp_ms')

    public SimplifiedTweet(long tweetId, String text, long userId, String userName, String language, long timestampMs) {

        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.language = language;
        this.timestampMs = timestampMs;

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

    public static Optional<SimplifiedTweet> fromJson(String jsonStr) {

        Optional<SimplifiedTweet> tweetopt = Optional.empty();

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

            SimplifiedTweet tweet = new SimplifiedTweet(tweetId, text, userId, userName, language, timestampMs);

            tweetopt = Optional.of(tweet);
        }

        return tweetopt;

    }
}