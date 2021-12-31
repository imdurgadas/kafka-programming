package in.durgadas.tech.kafka.es;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC = "twitter_tweets";
    public static final String GROUP_ID = "ElasticConsumerApp";
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private static final String ES_HOST = System.getenv("ES_HOST");
    private static final int ES_PORT = Integer.parseInt(System.getenv("ES_PORT"));
    private static final String ES_SCHEME = System.getenv("ES_SCHEME");
    private static final String ES_USERNAME = System.getenv("ES_USERNAME");
    private static final String ES_PASSWORD = System.getenv("ES_PASSWORD");

    public static void main(String[] args) throws IOException {
        try (RestHighLevelClient client = createClient()) {
            KafkaConsumer<String, String> consumer = createConsumer(TOPIC);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                logger.info("Received {} records", records.count());

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {
                    String id = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                            .source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);
                    // IndexResponse indexResponse = client.index(indexRequest);
                    // logger.info("Indexed tweet: {}", indexResponse.getId());
                }

                if (records.count() > 0) {
                    client.bulk(bulkRequest); // improve performance by batching

                    logger.info(
                            "Procesing of the receiving requests for the poll duration done, committing the offsets");
                    consumer.commitSync();
                    logger.info("Committed the offset...yaay");
                }

            }
        } catch (Exception e) {
            logger.error("Exception while polling", e);
        }

    }

    private static String extractIdFromTweet(String value) {
        return JsonParser.parseString(value).getAsJsonObject().get("id_str").getAsString();
    }

    public static RestHighLevelClient createClient() {

        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ES_USERNAME, ES_PASSWORD));

        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(ES_HOST, ES_PORT, ES_SCHEME))
                        .setHttpClientConfigCallback(
                                httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        Properties configs = new Properties();
        configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Manual commit of offsets
        configs.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        configs.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Collections.singleton(topic));

        return consumer;
    }
}
