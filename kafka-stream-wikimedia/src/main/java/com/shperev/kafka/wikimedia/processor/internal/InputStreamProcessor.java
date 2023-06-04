package com.shperev.kafka.wikimedia.processor.internal;

import org.apache.kafka.streams.kstream.KStream;

/**
 * The InputStreamProcessor interface is abstraction for processing kafka streams
 *
 * @param <K> type of the kafka serialization for message key
 * @param <V> type of the kafka serialization for message value
 */
public interface InputStreamProcessor<K, V> {

  /**
   * The processing method accept the kafka stream and processing the stream based on the defined
   * filtering criteria
   *
   * @param inputStream kafka stream
   */
  void process(KStream<K, V> inputStream);
}
