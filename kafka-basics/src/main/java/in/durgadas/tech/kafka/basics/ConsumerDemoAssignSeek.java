package in.durgadas.tech.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs)) {
            TopicPartition partitionToReadFrom = new TopicPartition(TOPIC, 0);
            consumer.assign(Collections.singleton(partitionToReadFrom));
            consumer.seek(partitionToReadFrom, 5);

            int messagesToRead = 5;
            boolean keepPolling = true;
            int messagesReadSoFar = 0;

            while (keepPolling) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    messagesReadSoFar++;
                    logger.info("Received message, key: {} , value: {} , partition: {}, offset: {}, timestamp: {} ",
                            record.key(), record.value(), record.partition(), record.offset(), record.timestamp());

                    if (messagesReadSoFar >= messagesToRead) {
                        keepPolling = false;
                        break;
                    }
                }
            }

            logger.info("Replaying done, application exiting");
        }
    }
}
