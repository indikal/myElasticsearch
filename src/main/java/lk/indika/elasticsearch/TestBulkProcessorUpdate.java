/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 * @author indika
 */
public class TestBulkProcessorUpdate {
    public static void main(String[] args) {
        updateProductChanges();
    }
    
    private static void updateProductChanges() {
        try {
            Long documentId = 2633529322L;
            Map<String, Object> fieldValues = new HashMap<String, Object>();
            fieldValues.put("eco", 3);
            
            Settings settings = Settings.settingsBuilder().put("cluster.name", "prodcat-dev").build();
                Client client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			
            if (null != documentId && documentId > 0) {
                BulkProcessor bulkProcessor = getBulkProcessor(client);
                
                System.out.println("Updating ducumentid: " + documentId);
                bulkProcessor.add(client.prepareUpdate("ehav-qa","products",documentId.toString())
                    .setDoc(fieldValues)
                    .request()
                );
    			
                //bulkProcessor.awaitClose(100, TimeUnit.MILLISECONDS);
                bulkProcessor.close();
                System.out.println("Elasticsearch index updated for eco.");
            } else {
                System.out.println("No products found to update elasticsearch index ...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static BulkProcessor getBulkProcessor(Client client) {
        return BulkProcessor.builder(
    	        client,  
    	        new BulkProcessor.Listener() {
    	            @Override
    	            public void beforeBulk(long executionId,
    	                                   BulkRequest request) {  } 

    	            @Override
    	            public void afterBulk(long executionId,
    	                                  BulkRequest request,
    	                                  BulkResponse response) {
    	            	if (response.hasFailures()) {
    	                    // process failures by iterating through each bulk response item
    	                       System.out.println("Error: synchronizing product changes! Err: " + response.buildFailureMessage());
    	                }
    	            } 

    	            @Override
    	            public void afterBulk(long executionId,
    	                                  BulkRequest request,
    	                                  Throwable failure) {  } 
    	        })
    	        .setBulkActions(1000) 
    	        .setBulkSize(new ByteSizeValue(20, ByteSizeUnit.MB)) 
    	        .setFlushInterval(TimeValue.timeValueSeconds(5)) 
    	        .setConcurrentRequests(1) 
    	        .build();
	}
}
