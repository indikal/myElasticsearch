package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class ElasticsearchClientFactory {

    private static final String DEFAULT_INDEX_NAME = "ehav-qa";
    private volatile static ElasticsearchClientFactory instance;
    private static Client client;
    private static String host;
    private static Integer port;
    private static String cluster;
    private static String document_type;
    private static final String DEFAULT_CLUSTER = "prodcat";
    private static final String DEFAULT_DOCTYPE = "products";

    private ElasticsearchClientFactory() {
        init();
    }

    public static ElasticsearchClientFactory getInstance() {
        if (null != instance && null != client) {
            System.out.println("Returning existing ElasticsearchClientFactory ...");
            return instance;
        }
        synchronized (ElasticsearchClientFactory.class) {
            if (null == instance || null == client) {
                System.out.println("Creating new ElasticsearchClientFactory ...");
                instance = new ElasticsearchClientFactory();
            }
        }
        return instance;
    }
    
    public SearchRequestBuilder prepareSearch() {
        return prepareSearch(DEFAULT_INDEX_NAME);
    }
    
    public SearchRequestBuilder prepareSearch(String index) {
        return client.prepareSearch(index);
    }
    
    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return client.prepareSearchScroll(scrollId);
    }
    
    public IndexRequestBuilder prepareIndex() {
        return prepareIndex(DEFAULT_INDEX_NAME);
    }
    
    public IndexRequestBuilder prepareIndex(String index) {
        return client.prepareIndex(index, document_type);
    }
    
    public UpdateRequestBuilder prepareUpdateRequest(String documentId) {
        return prepareUpdateRequest(DEFAULT_INDEX_NAME, documentId);
    }
    
    public UpdateRequestBuilder prepareUpdateRequest(String index, String documentId) {
        return client.prepareUpdate(index, document_type, documentId);
    }
    
    public BulkProcessor getBulkProcessor() {
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
    	        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(10), 3)) 
    	        .build();
	}

    private void init() {
        
        host = "localhost";
        String tmp_port = "9300";
        cluster = "prodcat-dev";
        document_type = "products";

        if (StringUtils.isEmpty(host)) {
            host = "localhost";
            System.out.println("Property not found: prodcat-elasticsearch-host. Set as localhost.");
        }

        if (StringUtils.isEmpty(tmp_port)) {
            port = 9300;
            System.out.println("Property not found: prodcat-elasticsearch-port. Set to 9300.");
        } else {
            try {
                port = Integer.parseInt(tmp_port);
            } catch (NumberFormatException e) {
                port = 9300;
            }
        }

        if (StringUtils.isEmpty(cluster)) {
            cluster = DEFAULT_CLUSTER;
            System.out.println("Property not found: prodcat-elasticsearch-cluster. Set to " + DEFAULT_CLUSTER);
        }

        if (StringUtils.isEmpty(document_type)) {
            document_type = DEFAULT_DOCTYPE;
            System.out.println("Property not found: prodcat-elasticsearch-doctype. Set to " + DEFAULT_DOCTYPE);
        }

        try {
            createClient(host, port, cluster);
        } catch (Exception e) {
            client = null;
            e.printStackTrace();
        }
    }

    private static void createClient(String host, Integer port, String cluster) throws Exception {
        // set the cluster name if you use one different than "elasticsearch":
        System.out.println("Creating Elasticsearch client ...");
        Settings settings = Settings.settingsBuilder().put("cluster.name", cluster).build();
        client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));

        System.out.println("Elasticsearch client created successfully.");
    }

    private void disconnect() {
        client.close();
        client = null;
    }
}
