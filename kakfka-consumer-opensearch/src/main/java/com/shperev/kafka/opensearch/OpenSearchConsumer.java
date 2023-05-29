package com.shperev.kafka.opensearch;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {

  private static Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class);

  public static void main(String[] args) throws IOException {

    final String wikimediaIndex = "wikimedia";
    final String wikimediaTopic = "wikimedia-recent-change";

    RestHighLevelClient openSearchClient = OpenSearchConnection.create();
    KafkaConsumer<String, String> wikimediaKafkaConsumer = WikimediaKafkaConsumer.create();

    createShutDownHook(wikimediaKafkaConsumer);

    try (openSearchClient;
        wikimediaKafkaConsumer) {
      boolean isIndexExists =
          openSearchClient
              .indices()
              .exists(new GetIndexRequest(wikimediaIndex), RequestOptions.DEFAULT);

      wikimediaKafkaConsumer.subscribe(Collections.singleton(wikimediaTopic));

      // Create index in OpenSearch
      if (!isIndexExists) {
        openSearchClient
            .indices()
            .create(new CreateIndexRequest(wikimediaIndex), RequestOptions.DEFAULT);
        logger.info("The wikimedia index has been created");
      }

      BulkRequest bulkRequest = new BulkRequest();

      // Insert record into OpenSearch
      while (true) {
        ConsumerRecords<String, String> records =
            wikimediaKafkaConsumer.poll(Duration.ofMillis(3000));
        int recordCounts = records.count();
        logger.info(String.format("Received %s records(s)", recordCounts));

        for (ConsumerRecord<String, String> record : records) {
          IndexRequest indexRequest = processKafkaRecord(wikimediaIndex, record);
          bulkRequest.add(indexRequest);
        }

        // Make bulk request to OpenSearch
        makeBulkRequest(openSearchClient, wikimediaKafkaConsumer, bulkRequest);
      }
    } catch (WakeupException e) {
      logger.info("Consumer is starting to shutdown");
    } catch (Exception e) {
      logger.error("Unexpected exception is in the consumer", e);
    } finally {
      wikimediaKafkaConsumer.close(); // close the consumer, this will also commit the offset
      logger.info("The consumer now gracefully shutdown");
      openSearchClient.close();
    }
  }

  private static void createShutDownHook(KafkaConsumer<String, String> wikimediaKafkaConsumer) {
    final Thread mainThread = Thread.currentThread();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                  wikimediaKafkaConsumer.wakeup();

                  // join main thread to allow execution of the code in main thread
                  try {
                    mainThread.join();
                  } catch (InterruptedException e) {
                    logger.error("Interrupted the main thread", e);
                  }
                }));
  }

  private static IndexRequest processKafkaRecord(
      String wikimediaIndex, ConsumerRecord<String, String> record) {
    String idempotentId = extractFromMessage(record.value());

    IndexRequest indexRequest =
        new IndexRequest(wikimediaIndex).source(record.value(), XContentType.JSON).id(idempotentId);

    return indexRequest;
  }

  private static void makeBulkRequest(
      RestHighLevelClient openSearchClient,
      KafkaConsumer<String, String> wikimediaKafkaConsumer,
      BulkRequest bulkRequest)
      throws IOException {
    if (bulkRequest.numberOfActions() > 0) {
      BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
      logger.info(
          String.format("Bulk response, response length: %s", bulkResponse.getItems().length));

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.error(e.getMessage());
      }

      wikimediaKafkaConsumer.commitSync();
      logger.info("Offsets have been committed!");
    }
  }

  private static String extractFromMessage(String value) {
    return JsonParser.parseString(value)
        .getAsJsonObject()
        .get("meta")
        .getAsJsonObject()
        .get("id")
        .getAsString();
  }
}
