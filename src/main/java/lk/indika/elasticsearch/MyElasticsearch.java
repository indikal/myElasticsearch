package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

public class MyElasticsearch {
	public static void main(String[] args) {
		try {
			// on startup
			//Client client = TransportClient.builder().build()
			//        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			        //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host2"), 9300));

			//set the cluster name if you use one different than "elasticsearch":
			Settings settings = Settings.settingsBuilder()
			        .put("cluster.name", "prodcat-test").build();
			Client client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.26.72.121"), 9300));
	        
			//Add transport addresses and do something with the client...
			//GetResponse response = client.prepareGet("products", "product", "126678").get();
			long start = System.currentTimeMillis();
			int page_no = 1;
			
            String regex = "(^?|.*\\|)(" + "433510" + "):.*";
            BoolQueryBuilder query = QueryBuilders.boolQuery();
			//query.must(QueryBuilders.regexpQuery("assortments", regex));
            //query.must(QueryBuilders.queryStringQuery("577*").field("supplierprodcode"));
            //query.must(QueryBuilders.wildcardQuery("supplierprodcode", "*577*"));
            //query.must(QueryBuilders.queryStringQuery("*2426542*").field("navigationnodepath"));
            //query.must(QueryBuilders.existsQuery("replacesitemdid"));

            Long[] arr = new Long[]{107468L,103908L,107521L,24674L,25430L,25543L,25555L,25696L,25831L,25833L,107555L,107592L,107673L,107723L,107747L,107794L,24592L,24598L,24697L,24811L};
            List<Long> dids = Arrays.asList(arr);
            BoolQueryBuilder commodityDids = QueryBuilders.boolQuery();
			commodityDids.minimumNumberShouldMatch(1);

			for (Long did : dids) {
				commodityDids.should(QueryBuilders.termQuery("commoditydid", did));
			}
			query.must(commodityDids);
            
            query.must(QueryBuilders.termQuery("pricelistactuality", 2));
            
			SearchResponse response = client.prepareSearch("ehav-qa")
			        .setTypes("products")
			        //.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			        .setQuery(query)                 // Query
			        //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
			        //.addSort("tradeitemdid", SortOrder.DESC)
			        //.setFrom(page_no)
			        .setSize(20)
			        //.setExplain(true)
			        .execute()
			        .actionGet();
			
			SearchHits shits = response.getHits();
			long total_recs = shits.getTotalHits();
			int i = page_no;
			System.out.println("Total Records: " + total_recs + "\n");
			
            String commDids = "1234";
			for (SearchHit hit : shits) {
				//System.out.println("Record " + i++ + " of " + total_recs + " [Product Did: " + hit.getId() + "]");
				
				Map<String, Object> fields = hit.getSource();
                commDids += "," + fields.get("tradeitemdid") + "L";
                System.out.print("tradeitemdid: " + fields.get("tradeitemdid"));
                System.out.print(", " + "supplierprodcode: " + fields.get("supplierprodcode"));
                System.out.print(", " + "prodname: " + fields.get("prodname"));
                System.out.print(", " + "supplieragreementno: " + fields.get("supplieragreementno"));
                System.out.println(", " + "buyerprodcode: " + fields.get("buyerprodcode"));
                System.out.println("navigationnodepath: " + fields.get("navigationnodepath"));
//				for (String key : fields.keySet()) {
//					System.out.println(key + ": " + fields.get(key));
//				}
				//System.out.println("\n----------------------------------------------------------\n ");
			}
			System.out.println("Total time taken: " + (System.currentTimeMillis() - start) + "ms");
			System.out.println("Commodity Dids: " + commDids);
			
			// on shutdown
			client.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
