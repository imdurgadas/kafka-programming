package in.durgadas.tech.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC = "first_topic";
    public static final String GROUP_ID = "DemoConsumerApp";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs)) {
            consumer.subscribe(Collections.singleton(TOPIC));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    logger.info("Received message, key: {} , value: {} , partition: {}, offset: {}, timestamp: {} ",
                            record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
                });
            }
        }

    }
}
