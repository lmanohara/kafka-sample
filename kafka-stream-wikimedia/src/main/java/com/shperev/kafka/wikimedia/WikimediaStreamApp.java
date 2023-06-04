package com.shperev.kafka.wikimedia;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaStreamApp {

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

  public static void main(String[] args) {}
}
