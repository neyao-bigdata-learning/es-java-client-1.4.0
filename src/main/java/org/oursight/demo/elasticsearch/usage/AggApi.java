package org.oursight.demo.elasticsearch.usage;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.List;
import java.util.Map;

/**
 * Created by neyao@github.com on 2016/4/19.
 */
public class AggApi {



    public static void aggregationByTerms(Client esClient, String index, String type, String aggFieldName) {
        SearchRequestBuilder searchRequestBuilder = esClient.prepareSearch(index).setTypes(type).setSize(0).
                addAggregation(AggregationBuilders.terms("readable_agg_name").field(aggFieldName).minDocCount(2).size(100));

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
}
