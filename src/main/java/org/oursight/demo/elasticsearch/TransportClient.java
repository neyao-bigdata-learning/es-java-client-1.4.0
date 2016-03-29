package org.oursight.demo.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * The TransportClient which uses version v1.4.0
 * Created by neyao@github.com on 2016/3/25.
 */
public class TransportClient {

    public static void main(String[] args) {
        // connect to a 1.7.0 ES, which will success
        connect("elasticsearch","221.122.121.96", 19300, "flume-bank-parsers");

        // connect to a 2.2.0 ES, which will fail
        connect("my-application","127.0.0.1", 9300, "megacorp");



    }

    /**
     * A simple method that will connect to ElasticSearch and execute a simple search, then print to result in standard output.<br><br>
     *
     * Please note that you COULD NOT use this method to connect to a version 2.2.0 ElasicSearch, otherwise the severside will throw excetpion.<br>
     *
     * <br>Known supported ElasticSearch version:<br>
     *     - v1.7.1<br>
     *
     * <br>Known unsupported ElasticSearch version:<br>
     *     - v2.2.0<br>
     *
     *
     * @param clusterName
     * @param address
     * @param port
     */
    public static void connect(String clusterName, String address, int port, String indexName) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "my-application").build();
        Client esClient = new org.elasticsearch.client.transport.TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9300));
        SearchResponse response = esClient.prepareSearch("megacorp").setSearchType(SearchType.QUERY_AND_FETCH).setSize(5).execute().actionGet();

        System.out.println("response.getHits().totalHits() = " + response.getHits().totalHits());
        System.out.println("response = " + response);

    }
}
