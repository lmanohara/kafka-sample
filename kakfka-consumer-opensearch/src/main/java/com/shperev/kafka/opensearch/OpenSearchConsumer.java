package com.shperev.kafka.opensearch;

import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OpenSearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class);

    public static void main(String[] args) throws IOException {

        final String wikimediaIndex = "wikimedia";

        RestHighLevelClient restHighLevelClient = OpenSearchConnection.create();

        try (restHighLevelClient) {
            boolean isIndexExists = restHighLevelClient.indices().exists(new GetIndexRequest(wikimediaIndex), RequestOptions.DEFAULT);

            if (!isIndexExists) {
                restHighLevelClient.indices().create(new CreateIndexRequest(wikimediaIndex), RequestOptions.DEFAULT);
                logger.info("The wikimedia index has been created");
            }
        }
    }
}
