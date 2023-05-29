package com.shperev.kafka.opensearch;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

        for (ConsumerRecord<String, String> record : records) {
          String idempotentId = extractFromMessage(record.value());

          IndexRequest indexRequest =
              new IndexRequest(wikimediaIndex)
                  .source(record.value(), XContentType.JSON)
                  .id(idempotentId);

          try {
            bulkRequest.add(indexRequest);
          } catch (Exception e) {
            logger.error(e.getMessage());
          }
        }

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
        }
      }
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
