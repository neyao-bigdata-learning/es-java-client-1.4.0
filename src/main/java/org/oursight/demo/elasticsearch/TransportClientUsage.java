package org.oursight.demo.elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.oursight.demo.elasticsearch.usage.SearchAPI;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * The TransportClient which uses version v1.4.0
 * Created by neyao@github.com on 2016/3/25.
 */
public class TransportClientUsage {

    public static void main(String[] args) {


        // connect to a 2.2.0 ES, which will fail
//        Client esClient = connect("my-application", "127.0.0.1", 9300, "megacorp");

        // connect to a 1.7.0 ES, which will success
//        Client esClient = connect("elasticsearch", "192.168.1.1", 9300);
        Client esClient = connect("elasticsearch", "221.122.121.96", 19300);

//        getSeveralDocs(esClient, "flume-bank-parsers" ,5);

        // getSingleDoc(esClient, "flume-bank-parsers", null, "00473676");

//        SearchAPI.search(esClient);
//        SearchAPI.searchByRawQueryString(esClient);
//        SearchAPI.listAllIndices(esClient);
//        SearchAPI.listFields(esClient, "flume-bank-parsers");
        SearchAPI.aggregationByTerms(esClient,"flume-bank-parsers", "flumetype", "snc_region_province");

        // ---
        esClient.close();

    }

    /**
     * A simple method that will connect to ElasticSearch and execute a simple search, then print to result in standard output.<br><br>
     * <p>
     * Please note that you COULD NOT use this method to connect to a version 2.2.0 ElasicSearch, otherwise the severside will throw excetpion.<br>
     * <p>
     * <br>Known supported ElasticSearch version:<br>
     * - v1.7.1<br>
     * <p>
     * <br>Known unsupported ElasticSearch version:<br>
     * - v2.2.0<br>
     *
     * @param clusterName
     * @param address
     * @param port
     */
    public static Client connect(String clusterName, String address, int port) {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        Client esClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(address, port));
        return esClient;
    }

    /**
     * To get a single document from ES
     *
     * @param esClient
     * @param indexName
     * @param type
     * @param id
     */
    public static void getSingleDoc(Client esClient, String indexName, @Nullable String type, String id) {
        GetResponse response = esClient.prepareGet(indexName, type, id).execute().actionGet();
        System.out.println("response = " + response);
        System.out.println("response.getSource() = " + response.getSource());
    }

    public static void getSeveralDocs(Client esClient, String indexName, int size) {
        SearchResponse response = esClient.prepareSearch(indexName).setSearchType(SearchType.QUERY_AND_FETCH).setSize(size).execute().actionGet();
        System.out.println("response.getHits().totalHits() = " + response.getHits().totalHits());
        System.out.println("response = " + response);
    }

    public static void createDocument(Client esClient) {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user", "kimchy");
        json.put("postDate", new Date());
        json.put("message", "trying out Elasticsearch");

        //esClient.prepareIndex("mega")

    }


}
