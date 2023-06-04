package com.shperev.kafka.wikimedia.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shperev.kafka.wikimedia.processor.internal.InputStreamProcessor;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.streams.kstream.*;

public class BotCountStreamBuilder implements InputStreamProcessor<String, String> {

  private static final String BOT_COUNT_STORE = "bot-count-store";
  private static final String BOT_COUNT_TOPIC = "wikimedia-stats-bots";

  private final ObjectMapper OBJECT_MAPPER;

  public BotCountStreamBuilder(ObjectMapper objectMapper) {
    this.OBJECT_MAPPER = objectMapper;
  }

  @Override
  public void process(KStream<String, String> inputStream) {
    inputStream
        .mapValues(
            changeJson -> {
              try {
                final JsonNode jsonNode = OBJECT_MAPPER.readTree(changeJson);
                if (jsonNode.get("bot").asBoolean()) {
                  return "bot";
                }
                return "non-bot";
              } catch (IOException e) {
                return "parse-error";
              }
            })
        .groupBy((key, botOrNot) -> botOrNot)
        .count(Materialized.as(BOT_COUNT_STORE))
        .toStream()
        .mapValues(
            (key, value) -> {
              final Map<String, Long> kvMap = Map.of(String.valueOf(key), value);
              try {
                return OBJECT_MAPPER.writeValueAsString(kvMap);
              } catch (JsonProcessingException e) {
                return null;
              }
            })
        .to(BOT_COUNT_TOPIC);
  }
}
