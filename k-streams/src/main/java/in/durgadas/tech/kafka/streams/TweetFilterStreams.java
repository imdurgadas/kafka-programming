package in.durgadas.tech.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TweetFilterStreams {

    private static final Logger logger = LoggerFactory.getLogger(TweetFilterStreams.class.getName());
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "twitter_tweets";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-1");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> inputTopic = streamsBuilder.stream(TOPIC);
        KStream<String,String> filteredStream  = inputTopic.filter(
                (k,tweet) -> extractUserFollowersInTweet(tweet) > 10000
        );

        filteredStream.to("important_tweets");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();

    }

    private static int extractUserFollowersInTweet(String tweet){
        try {
            return JsonParser.parseString(tweet).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
        }catch (NullPointerException e){
            return 0;
        }
    }
}
