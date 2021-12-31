package in.durgadas.tech.kafka.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getName());
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {
            for (int i = 0; i < 10; i++) {
                String key = "id_" + i;
                String value = "Kafka101 message: " + i;
                producer.send(new ProducerRecord<>(TOPIC, key, value), (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Error while producing message", exception);
                    } else {
                        logger.info(
                                "Produced message, key: {} , value: {} , partition: {}, offset: {}, timestamp: {} ",
                                key, value, metadata.partition(), metadata.offset(), metadata.timestamp());
                    }
                });
                producer.flush();
            }
        }
    }
}
