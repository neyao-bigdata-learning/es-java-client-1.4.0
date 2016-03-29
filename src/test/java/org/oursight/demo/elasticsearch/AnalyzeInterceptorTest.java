package org.oursight.demo.elasticsearch;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.flume.Context;
//import org.apache.flume.Event;
//import org.apache.flume.event.EventBuilder;
//import org.apache.flume.interceptor.Interceptor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import junit.framework.Assert;
import junit.framework.TestCase;

public class AnalyzeInterceptorTest extends TestCase {
	public static void testAnalyze() {
		double minClientScore = 30.0;
		String riskInfo;
		FilterBuilder innerRiskBuilder = FilterBuilders
				.boolFilter()
				.must(FilterBuilders.nestedFilter("nna_risks",
						FilterBuilders.existsFilter("nna_risks.ina_id")))
				.must(FilterBuilders.nestedFilter(
						"nna_clients",
						FilterBuilders
								.boolFilter()
								.must(FilterBuilders
										.existsFilter("nna_clients.ina_id"))
								.must(FilterBuilders.rangeFilter(
										"nna_clients.dna_score").gt(
										minClientScore))));
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		QueryBuilder qb = QueryBuilders.filteredQuery(queryBuilder,
				innerRiskBuilder);
		Map<String, String> resHeader = new HashMap<String, String>();
		try {
			Settings settings = ImmutableSettings.settingsBuilder()
			// .put("client.transport.sniff", true)
					.put("cluster.name", "elasticsearch").build();
			Client client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress("221.122.121.96", 19300));
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(System.currentTimeMillis());
			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH) + 1;
			int day = cal.get(Calendar.DAY_OF_MONTH);
			String strDay = String.format("%04d-%02d-%02d", year, month, day);
			SearchResponse scrollResp = client
					.prepareSearch("flume-" + strDay +"-content-*")
					.setSearchType(SearchType.SCAN).setScroll(new TimeValue(60000))
					.setQuery(qb)
					.setSize(10).execute().actionGet(); // 100 hits per shard will
														// be returned for each
														// scroll
			// Scroll until no hits are returned
			int hitsCount = 0;
			final String[] keys = { "scc_title", "inp_type" };
			while (true) {
				for (SearchHit hit : scrollResp.getHits().getHits()) {
					// Handle the hit...
					hitsCount++;
					Map<String, Object> source = hit.getSource();
					String body = "";
					Map<String, String> header = new HashMap<String, String>();
					header.put("action", "addContents");
					header.put("_index", hit.getIndex());
					header.put("_id", hit.getId());
					if (source.containsKey("scc_content"))
						body = source.get("scc_content").toString();
					for (String key : keys) {
						if (source.containsKey(key))
							header.put(key, source.get(key).toString());
					}
				}
				if (hitsCount > 2000)
					break;
				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
						.setScroll(new TimeValue(600000)).execute().actionGet();
				// Break condition: No hits are returned
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
