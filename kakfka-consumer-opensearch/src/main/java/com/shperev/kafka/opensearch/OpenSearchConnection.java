package com.shperev.kafka.opensearch;

import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.net.URI;

public class OpenSearchConnection {

    /**
     * Initiate client connection to the OpenSearch database
     *
     * @return {@link RestHighLevelClient}
     */
    public static RestHighLevelClient create() {
        String connString = "http://localhost:9200";

        URI connUri = URI.create(connString);

        return new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
    }
}
