package com.shperev.kafka.wikimedia.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shperev.kafka.wikimedia.processor.internal.InputStreamProcessor;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;

public class EventCountTimeSeriesBuilder implements InputStreamProcessor<String, String> {

  private static final String TIMESERIES_TOPIC = "wikimedia-stats-timeseries";
  private static final String TIMESERIES_STORE = "event-count-store";
  private final ObjectMapper OBJECT_MAPPER;

  public EventCountTimeSeriesBuilder(ObjectMapper objectMapper) {
    this.OBJECT_MAPPER = objectMapper;
  }

  @Override
  public void process(KStream<String, String> inputStream) {
    final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10));
    inputStream
        .selectKey((key, value) -> "key-to-group")
        .groupByKey()
        .windowedBy(timeWindows)
        .count(Materialized.as(TIMESERIES_STORE))
        .toStream()
        .mapValues(
            (readOnlyKey, value) -> {
              final Map<String, Object> kvMap =
                  Map.of(
                      "start_time", readOnlyKey.window().startTime().toString(),
                      "end_time", readOnlyKey.window().endTime().toString(),
                      "window_size", timeWindows.size(),
                      "event_count", value);
              try {
                return OBJECT_MAPPER.writeValueAsString(kvMap);
              } catch (JsonProcessingException e) {
                return null;
              }
            })
        .to(
            TIMESERIES_TOPIC,
            Produced.with(
                WindowedSerdes.timeWindowedSerdeFrom(String.class, timeWindows.size()),
                Serdes.String()));
  }
}
