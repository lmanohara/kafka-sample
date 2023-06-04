package com.shperev.kafka.wikimedia;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shperev.kafka.wikimedia.processor.BotCountStreamBuilder;
import com.shperev.kafka.wikimedia.processor.EventCountTimeSeriesBuilder;
import com.shperev.kafka.wikimedia.processor.WebsiteCountStreamBuilder;
import com.shperev.kafka.wikimedia.processor.internal.InputStreamProcessor;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaStreamApp {

  private static final String INPUT_TOPIC = "wikimedia-recent-change";
  private static Logger logger = LoggerFactory.getLogger(WikimediaStreamApp.class);
  private static Properties kafkaProperties;

  static {
    kafkaProperties = new Properties();

    kafkaProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-stat-application");
    kafkaProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    kafkaProperties.setProperty(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    kafkaProperties.setProperty(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
  }

  public static void main(String[] args) {

    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> changeJsonStream = builder.stream(INPUT_TOPIC);

    final ObjectMapper objectMapper = new ObjectMapper();

    InputStreamProcessor<String, String> botCountStreamBuilder =
        new BotCountStreamBuilder(objectMapper);
    botCountStreamBuilder.process(changeJsonStream);

    InputStreamProcessor<String, String> eventCountTimeSeriesBuilder =
        new EventCountTimeSeriesBuilder(objectMapper);
    eventCountTimeSeriesBuilder.process(changeJsonStream);

    InputStreamProcessor<String, String> websiteCountStreamBuilder =
        new WebsiteCountStreamBuilder(objectMapper);

    websiteCountStreamBuilder.process(changeJsonStream);

    final Topology appTopology = builder.build();
    logger.info("Topology: {}", appTopology.describe());
    KafkaStreams streams = new KafkaStreams(appTopology, kafkaProperties);
    streams.start();
  }
}
