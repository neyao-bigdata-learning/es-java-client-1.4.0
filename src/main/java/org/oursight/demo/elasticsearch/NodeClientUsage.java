package org.oursight.demo.elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by neyao@github.com on 2016/3/25.
 */
public class NodeClientUsage {

    public static Node esNode = NodeBuilder.nodeBuilder().clusterName("my-application").node();
    public static Client esClient = esNode.client();

    public static void main(String[] args) {


        // Write data to ES
        //writeToES();

        // Show the use of GET request
        getFromES();


    }

    public static void getFromES() {
        GetResponse getResponse = esClient.prepareGet("megacorp","employee","10").execute().actionGet();
//        GetResponse getResponse = esClient.prepareGet("kodcucom","article","1").execute().actionGet();
        //GetResponse getResponse = esClient.prepareGet("kodcucom","article", "2").execute().actionGet();
        Map source =  getResponse.getSource();

        System.out.println("source = " + source);
        System.out.println("getResponse.getIndex() = " + getResponse.getIndex());
        System.out.println("getResponse.getType() = " + getResponse.getType());
        System.out.println("getResponse.getFields() = " + getResponse.getFields());
    }

    public static Map<String, Object> putJsonDocument(String title, String content, Date postDate,
                                                      String[] tags, String author){
        Map<String, Object> jsonDocument = new HashMap<String, Object>();
        jsonDocument.put("title", title);
        jsonDocument.put("conten", content);
        jsonDocument.put("postDate", postDate);
        jsonDocument.put("tags", tags);
        jsonDocument.put("author", author);
        return jsonDocument;
    }

    public static void writeToES() {
        esClient.prepareIndex("kodcucom", "article")
                .setSource(putJsonDocument("ElasticSearch: Java API",
                        "ElasticSearch provides the Java API, all operations "
                                + "can be executed asynchronously using a client object.",
                        new Date(),
                        new String[]{"elasticsearch"},
                        "zhangsan")).execute().actionGet();

        esClient.prepareIndex("kodcucom", "article")
                .setSource(putJsonDocument("title 2222",
                        "content 22222"
                                + "can be executed asynchronously using a client object.",
                        new Date(),
                        new String[]{"elasticsearch"},
                        "yaonengjun")).execute().actionGet();

        esClient.prepareIndex("kodcucom", "article")
                .setSource(putJsonDocument("title 333333333",
                        "content 33333333"
                                + "can be executed asynchronously using a client object.",
                        new Date(),
                        new String[]{"elasticsearch"},
                        "lisi")).execute().actionGet();


    }

   public static void searchDocument(Client client, String index, String type,
                                      String field, String value){
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_AND_FETCH)
//                .setQuery(QueryBuilders.fieldQuery(field, value))
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();
        SearchHit[] results = response.getHits().getHits();
        System.out.println("Current results: " + results.length);
        for (SearchHit hit : results) {
            System.out.println("------------------------------");
            Map<String,Object> result = hit.getSource();
            System.out.println(result);
        }
    }



}
