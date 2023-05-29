package com.shperev.kafka.opensearch;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class WikimediaKafkaConsumer {

  /**
   * Initiate kafka consumer
   *
   * @return {@link KafkaConsumer}
   */
  public static KafkaConsumer<String, String> create() {
    // consumer group
    String consumerGroupId = "wikimedia-application";

    // create kafka producer properties
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

    // consumer properties
    consumerProperties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
    consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // create consumer
    return new KafkaConsumer<>(consumerProperties);
  }
}
