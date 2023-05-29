package com.shperev.kafka.sample;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSampleConsumer {

  private static final Logger logger = LoggerFactory.getLogger(KafkaSampleProducer.class.getName());

  public static void main(String[] args) {

    // consumer group
    String consumerGroupId = "java-application";

    // create kafka producer properties
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty("bootstrap.servers", "127.0.0.1:9092");

    // consumer properties
    consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
    consumerProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
    consumerProperties.setProperty("group.id", consumerGroupId);
    consumerProperties.setProperty("auto.offset.reset", "earliest");

    // create consumer
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

    kafkaConsumer.subscribe(List.of("demo-topic"));

    final Thread mainThread = Thread.currentThread();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                  kafkaConsumer.wakeup();

                  // join main thread to allow execution of the code in main thread
                  try {
                    mainThread.join();
                  } catch (InterruptedException e) {
                    logger.error("Interrupted the main thread", e);
                  }
                }));

    try {
      // poll for data
      while (true) {

        logger.info("polling");

        ConsumerRecords<String, String> consumerRecords =
            kafkaConsumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
          logger.info(
              String.format(
                  "Key: %s, Value: %s, Partition: %s, Offset: %s",
                  consumerRecord.key(),
                  consumerRecord.value(),
                  consumerRecord.partition(),
                  consumerRecord.offset()));
        }
      }
    } catch (WakeupException e) {
      logger.info("Consumer is starting to shutdown");
    } catch (Exception e) {
      logger.error("Unexpected exception is in the consumer", e);
    } finally {
      kafkaConsumer.close(); // close the consumer, this will also commit the offset
      logger.info("The consumer now gracefully shutdown");
    }
  }
}
