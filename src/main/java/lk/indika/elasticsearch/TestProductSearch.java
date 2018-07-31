/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;
import scala.collection.mutable.ArrayLike;

/**
 *
 * @author indika
 */
public class TestProductSearch {
    public static void main(String[] args) {
        //findOrderableProducts();
        //findBySupplierDid();
        //findBySupplierItemId();
        //findByBuyerItemId();
        findByQuery();
        //findByAgreementDid();
        //findByItemIds();
    }
    
    public static void findBySupplierDid() {
        ProductSearchDTO searchDTO = new ProductSearchDTO();
        searchDTO.setBuyerDid(29320L);
		
        findProducts(searchDTO, "localhost");
    }
    
    public static void findOrderableProducts() {
        ProductSearchDTO searchDTO = new ProductSearchDTO();
        searchDTO.setIsOrderable(Boolean.TRUE);
		searchDTO.setBuyerDid(29320L);
        
        findProducts(searchDTO, "localhost");
    }
    
    public static void findBySupplierItemId() {
        ProductSearchDTO searchDTO = new ProductSearchDTO();
		searchDTO.setSupplierItemId("577338D");
		
        findProducts(searchDTO, "localhost");
    }
    
    public static void findByBuyerItemId() {
        ProductSearchDTO searchDTO = new ProductSearchDTO();
		searchDTO.setBuyerItemId("ES1_577338D");
		
        findProducts(searchDTO, "localhost");
    }
    
    public static void findByQuery() {
        ProductSearchDTO searchDTO = new ProductSearchDTO();
        //"Salto {}()[]:\".+*?~^!$|/\\ klar 35cl 6"
		searchDTO.setQuery("Salto {}()[]:\".+*?~^!$|/\\ klar 35cl 6");
		
        findProducts(searchDTO, "localhost");
    }
    
    public static void findByAgreementDid() {
        ProductSearchDTO searchDTO = new ProductSearchDTO();
		searchDTO.setAgreementDids(Arrays.asList(2115193L));
		
        findProducts(searchDTO, "localhost");
    }
    
    public static void findByItemIds() {
        ProductSearchDTO searchDTO = new ProductSearchDTO();
		searchDTO.setItemIds(Arrays.asList("577338D"));
		
        findProducts(searchDTO, "localhost");
    }
    
    public static void findProducts(ProductSearchDTO searchDTO, String tenant) {
        try {
        	long start = System.currentTimeMillis();
            ElasticsearchClientFactory factory = ElasticsearchClientFactory.getInstance();

    		BoolQueryBuilder query = ElasticHelper.getFindProductsQuery(searchDTO);
    		
            //now get all documents matching the search
            //1000 hits per shard will be returned for each scroll
            SearchResponse response = factory.prepareSearch("ehav-qa")
                .setTypes("products")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(query)
				.addSort("documentid", SortOrder.ASC)
                //.addSort("pricelistdid", SortOrder.DESC)
                .setFrom(0)
                .setSize(200)
                .setExplain(false)
                .execute()
                .actionGet();
		
            SearchHits shits = response.getHits();

            long total_recs = shits.getTotalHits();
            
            if (total_recs > 0) {
                System.out.println("Total Records: " + total_recs + "\n");

                for (SearchHit hit : shits) {
                    //System.out.println("Record " + i++ + " of " + total_recs + " [Product Did: " + hit.getId() + "]");

                    Map<String, Object> fields = hit.getSource();
                    System.out.print("tradeitemdid: " + fields.get("tradeitemdid"));
                    System.out.print(", " + "supplierprodcode: " + fields.get("supplierprodcode"));
                    System.out.print(", " + "prodname: " + fields.get("prodname"));
                    System.out.print(", " + "suppliername: " + fields.get("suppliername"));
                    System.out.println(", " + "buyerprodcode: " + fields.get("buyerprodcode"));
                    
                    /**List groups = (ArrayList) fields.get("agreementgroups");
                    System.out.println("agreementgroups: [");
                    for (Object g : groups) {
                        System.out.println("{");
                        HashMap group = (HashMap) g;
                        for (Object key : group.keySet()) {
                            System.out.println("\t" + key + ": " + group.get(key));
                        }
                        System.out.println("},");
                    }
                    System.out.println("]");**/
                    //System.out.println("agreementgroups: " + fields.get("agreementgroups"));
                    
//                    for (String key : fields.keySet()) {
//                        System.out.println(key + ": " + fields.get(key));
//                    }
//                    System.out.println("\n----------------------------------------------------------\n ");
                }
                System.out.println("Total time taken: " + (System.currentTimeMillis() - start) + "ms");

    		} else {
                System.out.println("No documents found in elasticsearch ...");
            }
            //System.out.println("====================> Time taken to sync data to Elasticsearch: " + (System.currentTimeMillis() - start) + "ms");
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
}
