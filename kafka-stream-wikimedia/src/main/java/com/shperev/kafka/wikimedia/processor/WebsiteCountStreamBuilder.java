package com.shperev.kafka.wikimedia.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;

public class WebsiteCountStreamBuilder implements Consumer<KStream<String, String>> {
  private static final String WEBSITE_COUNT_STORE = "website-count-store";
  private static final String WEBSITE_COUNT_TOPIC = "wikimedia-stats-website";
  private final ObjectMapper OBJECT_MAPPER;

  public WebsiteCountStreamBuilder(ObjectMapper objectMapper) {
    this.OBJECT_MAPPER = objectMapper;
  }

  @Override
  public void accept(KStream<String, String> inputStream) {
    final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L));
    inputStream
        .selectKey(
            (k, changeJson) -> {
              try {
                final JsonNode jsonNode = OBJECT_MAPPER.readTree(changeJson);
                return jsonNode.get("server_name").asText();
              } catch (IOException e) {
                return "parse-error";
              }
            })
        .groupByKey()
        .windowedBy(timeWindows)
        .count(Materialized.as(WEBSITE_COUNT_STORE))
        .toStream()
        .mapValues(
            (key, value) -> {
              final Map<String, Object> kvMap = Map.of("website", key.key(), "count", value);
              try {
                return OBJECT_MAPPER.writeValueAsString(kvMap);
              } catch (JsonProcessingException e) {
                return null;
              }
            })
        .to(
            WEBSITE_COUNT_TOPIC,
            Produced.with(
                WindowedSerdes.timeWindowedSerdeFrom(String.class, timeWindows.size()),
                Serdes.String()));
  }
}
