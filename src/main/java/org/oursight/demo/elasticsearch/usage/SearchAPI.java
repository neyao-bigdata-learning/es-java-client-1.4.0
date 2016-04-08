package org.oursight.demo.elasticsearch.usage;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.json.JSONObject;

import java.util.Map;

/**
 * Created by neyao@github.com on 2016/4/8.
 */
public class SearchAPI {

    public static void search(Client esClient) {
        // see below link for more details
        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.4/search.html
//        SearchResponse searchResponse = esClient.prepareSearch("flume-bank-parsers", "flume-weixin-data")
        SearchResponse searchResponse = esClient.prepareSearch("flume-bank-parsers")
                .setTypes("flumetype")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("scc_lastaddress", "北京"))
                .setFrom(0).setSize(10)
                .setExplain(true)
                .execute().
                        actionGet();
       // System.out.println("searchResponse = " + searchResponse);

        iterateResponse(searchResponse, "scc_lastaddress");

    }

    /**
     * see below link for more details
     * http://teknosrc.com/execute-raw-elasticsearch-query-using-transport-client-java-api/
     *
     * @param esClient
     */
    public static void searchByRawQueryString(Client esClient) {
        String queryString = "{\"query\" : {\"match\" : {\"scc_lastaddress\" : \"北京\"}}}";
        JSONObject queryStringObject = new JSONObject(queryString); // 只是为了确保格式合法，实际上下边直接传queryString也是可以的


        SearchResponse searchResponse = esClient.prepareSearch("flume-bank-parsers")
                .setTypes("flumetype") // not necessary
                .setSource(queryStringObject.toString())
                .setFrom(0).setSize(10)
                .execute()
                .actionGet();

        iterateResponse(searchResponse, "scc_lastaddress");
    }

    private static void iterateResponse(SearchResponse searchResponse) {
        iterateResponse(searchResponse, null);
    }

    private static void iterateResponse(SearchResponse searchResponse, String fieldNameWantsToShow) {
        //System.out.println("searchResponse = " + searchResponse);

        if (searchResponse == null) {
            System.out.println("SearchResponse is null, will return directly");
            return;
        }

        SearchHits hits = searchResponse.getHits();

        System.out.println("hits.totalHits() = " + hits.totalHits());

        for (int i = 0; i < hits.totalHits(); i++) {
            SearchHit hit = hits.getAt(i);

            //System.out.println(i + " hit = " + hit.getSource());

            //Map<String, SearchHitField> responseFields = hits.getAt(i).getFields();
            Map _source = hit.getSource();
            //System.out.println("_source : " + _source);


            if (fieldNameWantsToShow != null) {
                Object value = _source.get(fieldNameWantsToShow);
                if (value == null) {
                    System.out.println(i + " can not find field [" + fieldNameWantsToShow + "] ");
                    continue;
                }
                System.out.println(fieldNameWantsToShow + ": " + value);
            } else {
                System.out.println("_source : " + _source);
                System.out.println();
            }


        }
    }
}
