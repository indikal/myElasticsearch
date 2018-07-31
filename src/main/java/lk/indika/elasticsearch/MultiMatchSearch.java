package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.util.Map;

import org.apache.lucene.queries.TermFilter;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;

public class MultiMatchSearch {
	public static void main(String[] args) {
		try {
			// on startup
			//Client client = TransportClient.builder().build()
			//        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			        //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));

			//set the cluster name if you use one different than "elasticsearch":
			Settings settings = Settings.settingsBuilder()
			        .put("cluster.name", "prodcat_inli-dev").build();
			Client client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
	        
			//Add transport addresses and do something with the client...
			//GetResponse response = client.prepareGet("products", "product", "126678").get();
			long start = System.currentTimeMillis();
			int page_no = 0;
			
			SearchResponse response = client.prepareSearch("products")
			        .setTypes("product")
			        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			        .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("navigationnodeid", 2392987))
							.must(QueryBuilders.queryStringQuery("*socker*")
					        		.field("prodname")
					        		.field("supplierprodcode")
					        		.field("buyerprodcode")
					        		.field("supplieragreementno"))
							.must(QueryBuilders.rangeQuery("price").from(100).to(150)))
			        		//.must(QueryBuilders.termsQuery("assortmentdids", new long[]{1l, 2l, 3l}))
			        /*.setQuery(QueryBuilders.queryStringQuery("*socker*")
			        		.field("prodname")
			        		.field("supplierprodcode")
			        		.field("buyerprodcode")
			        		.field("supplieragreementno"))*/                 // Query
			        //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
			        /*.setPostFilter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("navigationnodeid", 2392987))
			        										.must(QueryBuilders.queryStringQuery("*socker*")
			        								        		.field("prodname")
			        								        		.field("supplierprodcode")
			        								        		.field("buyerprodcode")
			        								        		.field("supplieragreementno"))
			        										.must(QueryBuilders.rangeQuery("price").from(100).to(200)))*/
			        .setFrom(page_no)
			        .setSize(20)
			        .setExplain(true)
			        .execute()
			        .actionGet();
			
			SearchHits shits = response.getHits();
			long total_recs = shits.getTotalHits();
			int i = page_no;
			System.out.println("Total Records: " + total_recs + "\n");
			
			for (SearchHit hit : shits) {
				System.out.println("Record " + ++i + " of " + total_recs + " [Product Did: " + hit.getId() + "]");
				
				Map<String, Object> fields = hit.getSource();
				for (String key : fields.keySet()) {
					System.out.println(key + ": " + fields.get(key));
				}
				System.out.println("\n----------------------------------------------------------\n ");
			}
			System.out.println("Total time taken: " + (System.currentTimeMillis() - start) + "ms");
			//System.out.println("Product Did: " + hits[0].getId());
			
			// on shutdown
			client.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
