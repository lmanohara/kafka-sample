package com.shperev.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";

        // create kafka producer properties
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // producer properties
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");

        // crate the kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);

        String topic = "wikimedia-recent-change";

        BackgroundEventHandler wikimediaChangeHandler = new WikimediaChangeHandler(kafkaProducer, topic);

        String wikimediaUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(wikimediaUrl));

        BackgroundEventSource.Builder backGroundEventSourceBuilder = new BackgroundEventSource.Builder(wikimediaChangeHandler, eventSourceBuilder);

        BackgroundEventSource backgroundEventSource = backGroundEventSourceBuilder.build();

        backgroundEventSource.start();

        TimeUnit.MINUTES.sleep(1);
    }
}
