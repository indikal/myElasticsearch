package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;

public class MyElasticAggregation {
	public static void main(String[] args) {
		Settings settings = Settings.settingsBuilder().put("cluster.name", "prodcat_inli-dev").build();
		Client client;
		try {
			client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

			/*
			 * SearchResponse response=
			 * client.prepareSearch("yourindex").setQuery(QueryBuilders.
			 * filteredQuery(QueryBuilders.matchAllQuery(),
			 * FilterBuilders.andFilter(
			 * FilterBuilders.termFilter("server","x"),
			 * FilterBuilders.termFilter("dt_time","x") ))).addAggregation(
			 * AggregationBuilders.terms("dt_timeaggs").field("dt_time").size(
			 * 100).subAggregation(
			 * AggregationBuilders.terms("cpu_aggs").field("cpu").size(100) )
			 * ).setSize(0).get();
			 */

			long start = System.currentTimeMillis();
			AggregationBuilder agg_node = AggregationBuilders.terms("agg_node").field("navigationnodeid").size(100);
			AggregationBuilder agg_sup = AggregationBuilders.terms("agg_sup").field("supplierid").size(100);
			SearchResponse response = client.prepareSearch("products")
					.setSize(0)
					.setTypes("product")
					.setQuery(QueryBuilders.termQuery("category", "dryck"))
					.addAggregation(agg_node)
					.addAggregation(agg_sup)
					.execute()
					.actionGet();

			System.out.println("Total Records: " + response.getHits().getTotalHits() + "\n");
			
			Terms aggTerms_node = response.getAggregations().get("agg_node");
			for (Bucket bucket : aggTerms_node.getBuckets()) {
				System.out.println("Key: " + bucket.getKey() + " Value: " + bucket.getDocCount());
			}
			
			System.out.println("-------------------------------------------------------------------");
			Terms aggTerms_sup = response.getAggregations().get("agg_sup");
			for (Bucket bucket : aggTerms_sup.getBuckets()) {
				System.out.println("Key: " + bucket.getKey() + " Value: " + bucket.getDocCount());
			}
			
			System.out.println("Total time taken: " + (System.currentTimeMillis() - start) + "ms");
			
			// on shutdown
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
