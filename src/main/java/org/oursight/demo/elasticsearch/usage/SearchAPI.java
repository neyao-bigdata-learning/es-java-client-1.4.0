package org.oursight.demo.elasticsearch.usage;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by neyao@github.com on 2016/4/8.
 */
public class SearchAPI {

    /**
     * Belone search is the same as:
      {
         "query" : {
             "match" : {
                 "scc_lastaddress" : "北京"
             }
         }
     }
     * @param esClient
     */
    public static void search(Client esClient) {
        // see below link for more details
        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.4/search.html
//        SearchResponse searchResponse = esClient.prepareSearch("flume-bank-parsers", "flume-weixin-data")
        SearchResponse searchResponse = esClient.prepareSearch("flume-bank-parsers")
                .setTypes("flumetype")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("scc_lastaddress", "北京"))
                .setFrom(0).setSize(1)
                .setExplain(true)
                .execute().
                        actionGet();
        // System.out.println("searchResponse = " + searchResponse);

        iterateResponse(searchResponse, "scc_lastaddress");

    }
    
    public static void searchWeixinGzh(Client esClient) {
        // see below link for more details
        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.4/search.html
//        SearchResponse searchResponse = esClient.prepareSearch("flume-bank-parsers", "flume-weixin-data")
        SearchResponse searchResponse = esClient.prepareSearch("flume-weixin-data")
                .setTypes("weixin-data")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("sn_enName", "北京"))
                .setFrom(0).setSize(1)
                .setExplain(true)
                .execute().
                        actionGet();
        // System.out.println("searchResponse = " + searchResponse);

        iterateResponse(searchResponse);

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
            	System.out.println("===================");
            	System.out.println("_source: " + _source);
            	System.out.println("===================");
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

    public static void listAllIndices(Client esClient) {
        String[] allIndices = esClient.admin().cluster().prepareState().execute().actionGet().getState().getMetaData().concreteAllIndices();

        if (allIndices == null) {
            System.out.println("Indices is null");
            return;
        }

        for (String index : allIndices) {
            System.out.println(index);
        }
        System.out.println("-------------------");
        System.out.println("All count: " + allIndices.length);
        System.out.println("-------------------");
    }

    public static void listFields(Client esClient, String index) {
        ClusterState clusterState = esClient.admin().cluster().prepareState().setIndices(index).execute().actionGet().getState();
        IndexMetaData indexMetaData = clusterState.getMetaData().index(index);
        MappingMetaData mappingMetaData = indexMetaData.mapping("flumetype");

        Map map = null;

        try {
            map = mappingMetaData.getSourceAsMap();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("map = " + map);
        List<String> fieldList = new ArrayList<String>();
        fieldList = getList("", map);

        System.out.println("Field List:");
        for (String field : fieldList) {
            System.out.println(field);
        }
        System.out.println();
        System.out.println("-------------------------");
        System.out.println("fieldList.size() = " + fieldList.size());
        System.out.println("-------------------------");
    }

    private static List<String> getList(String fieldName, Map<String, Object> mapProperties) {
        List<String> fieldList = new ArrayList<String>();
        Map<String, Object> map = (Map<String, Object>) mapProperties.get("properties");
        Set<String> keys = map.keySet();
        for (String key : keys) {
            if (((Map<String, Object>) map.get(key)).containsKey("type")) {
                fieldList.add(fieldName + "" + key);
            } else {
                List<String> tempList = getList(fieldName + "" + key + ".", (Map<String, Object>) map.get(key));
                fieldList.addAll(tempList);
            }
        }
        return fieldList;
    }

    /**
     * see below links for more details:
     * https://www.elastic.co/guide/en/elasticsearch/client/java-api/1.4/_bucket_aggregations.html
     *
     * @param esClient
     * @param index
     * @param type
     * @param aggFieldName
     */
    public static void aggregationByTerms(Client esClient, String index, String type, String aggFieldName) {
        SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(index).setTypes(type).setSize(0).
                addAggregation(AggregationBuilders.terms("aggName_test").field(aggFieldName));

//        SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(index).setTypes(type).setSize(0).
//                addAggregation(AggregationBuilders.global("aggName_test_1").subAggregation(AggregationBuilders.terms("aggName_test_2").field(aggFieldName)));


        System.out.println("searchRequestBuilder = " + searchRequestBuilder.toString());

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        //Global agg = searchResponse.getAggregations().get("aggName_test");
        //System.out.println("agg.getDocCount(): " + agg.getDocCount()); // get the "total" in "hits" of json

        System.out.println("searchResponse = " + searchResponse);

        Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
        StringTerms terms = (StringTerms) aggMap.get("aggName_test");
        List bucketList = terms.getBuckets();

        System.out.println("aggMap = " + aggMap);
        //System.out.println("bucketList = " + bucketList);

        for (Object bucket : bucketList) {
            bucket = (Terms.Bucket)bucket;
            System.out.println( ((Terms.Bucket) bucket).getKey() +": " + ((Terms.Bucket) bucket).getDocCount());
        }
    }

    /**
     * Equivalent code of query: SearchAPI.nestedFilter.json
     * @param esClient
     */
    public static void searchByNestedFilter(Client esClient, String index, String type) {
        FilterBuilder filterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.nestedFilter("nna_risks",FilterBuilders.existsFilter("nna_risks.ina_id")));
        System.out.println("filterBuilder.toString() = \r\n" + filterBuilder.toString());
        SearchResponse response = esClient.prepareSearch(index).setTypes(type).setSize(1).setPostFilter(filterBuilder).execute().actionGet();
        iterateResponse(response);

        QueryBuilder queryBuilder = QueryBuilders.filteredQuery(null, FilterBuilders.nestedFilter("nna_risks", FilterBuilders.boolFilter().must(FilterBuilders.existsFilter("nna_risks.ina_id"))));
        System.out.println("queryBuilder\r\n = " + queryBuilder);
        response = esClient.prepareSearch(index).setTypes(type).setSize(1).setQuery(queryBuilder).execute().actionGet();
        iterateResponse(response);


    }

    /**
     * Equivalent code of query: SearchAPI.rangeFilter.json
     * @param esClient
     */
    public static void rangeFilter(Client esClient) {
        QueryBuilder queryBuilder = QueryBuilders.filteredQuery(null, FilterBuilders.rangeFilter("tfp_save_time").gte("2016-04-12 00:00:00").lte("2016-04-12 23:00:00"));
        System.out.println("queryBuilder\r\n = " + queryBuilder);
        SearchResponse response = esClient.prepareSearch("flume-2016-04-*-content-news").setTypes("flumetype").setSize(10).setQuery(queryBuilder).execute().actionGet();
        iterateResponse(response);
    }

}
