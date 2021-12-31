package in.durgadas.tech.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TweetProducer {

    public static final Logger logger = LoggerFactory.getLogger(TweetProducer.class.getName());

    public static final String CONSUMER_KEY =  System.getenv("CONSUMER_KEY");
    public static final String CONSUMER_SECRET =  System.getenv("CONSUMER_SECRET");
    public static final String TOKEN =  System.getenv("TOKEN");
    public static final String TOKEN_SECRET =  System.getenv("TOKEN_SECRET");

    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TWITTER_TWEETS_TOPIC = "twitter_tweets";

    public static final List<String> terms = Lists.newArrayList("india", "cricket", "politics");

    public static void main(String[] args) {
        new TweetProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();

        while (!client.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    producer.send(new ProducerRecord<>(TWITTER_TWEETS_TOPIC, msg), (metadata, exception) -> logger.info("Tweet produced, partition: {} , offset: {}", metadata.partition(), metadata.offset()));
                }
            } catch (InterruptedException e) {
                logger.error("Exception while polling for message", e);
            }
        }

        client.stop();
        producer.close();

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);

        return new ClientBuilder().name("Hosebird-Client-01")
                .hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue)).build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties configs = new Properties();
        configs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Safe idempotent producer
        configs.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configs.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        configs.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        configs.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        configs.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");

        // Configuration for high throughput
        configs.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        configs.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        configs.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024)); // 32 KB Batch size instead of
        // default 16KB

        return new KafkaProducer<>(configs);
    }
}