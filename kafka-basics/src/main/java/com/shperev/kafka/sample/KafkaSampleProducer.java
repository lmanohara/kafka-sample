package com.shperev.kafka.sample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaSampleProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSampleProducer.class.getName());

    public static void main(String[] args) {

        // create kafka producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // producer properties
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("batch.size", "400");

        // crate the kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);

        String topicName = "demo-topic";

        // send data
        for (int i = 1; i <= 2; i++) {
            for (int j = 1; j <= 10; j++) {

                String key = String.format("id_%s", j);
                String value = String.format("sample value %s", j);

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topicName, key, value);

                kafkaProducer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info(String.format("Key: %s | Partition: %s", key, metadata.partition()));
                    } else {
                        logger.error(exception.getMessage());
                    }
                });

            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        // send all data and block until done - sync
        kafkaProducer.flush();

        kafkaProducer.close();
    }
}
