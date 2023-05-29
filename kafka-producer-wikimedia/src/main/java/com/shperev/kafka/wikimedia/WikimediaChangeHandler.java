package com.shperev.kafka.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements BackgroundEventHandler {

  private final KafkaProducer<String, String> kafkaProducer;
  private final String topic;
  private Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class);

  public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
    this.kafkaProducer = kafkaProducer;
    this.topic = topic;
  }

  @Override
  public void onOpen() {}

  @Override
  public void onClosed() {
    kafkaProducer.close();
  }

  @Override
  public void onMessage(String event, MessageEvent messageEvent) {
    logger.info(String.format("Wikimedia change data: %s", messageEvent.getData()));
    kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
  }

  public void onComment(String comment) {}

  @Override
  public void onError(Throwable t) {}
}
