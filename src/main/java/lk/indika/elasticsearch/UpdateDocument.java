package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class UpdateDocument {

	public static void main(String[] args) {
		try {
			String itemDid = "2426229320";
			// set the cluster name if you use one different than "elasticsearch":
			Settings settings = Settings.settingsBuilder().put("cluster.name", "prodcat-dev").build();
			Client client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			
			UpdateRequest updateRequest = new UpdateRequest("ehav-test", "products", itemDid)
			        .doc(jsonBuilder()
			            .startObject()
			                //.field("assortmentdids", "2433509,2433510,2433511,2433512,2433513")
                            .field("authorizerdids", "26793,29320,29321,29244,29322")
			                //.field("assortmentid", "1234567")
			            .endObject());
			client.update(updateRequest).get();
			System.out.println("Updated the document with ID: " + itemDid);
			
			//get the updated item to test
			GetResponse get = client.prepareGet("ehav-test", "products", itemDid).get();
			Map<String, Object> fields = get.getSource();
			for (String key : fields.keySet()) {
				System.out.println(key + ": " + fields.get(key));
			}

			// on shutdown
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

    private void createUpdateDocuments() throws Exception {
        Settings settings = Settings.settingsBuilder().put("cluster.name", "prodcat-dev").build();
		Client client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
		
        //client.prepareIndex("details", "<type_name>", "<id>").setSource(putJsonDocumentString(Key, Value)).execute().actionGet();
        
        String json = "{" +
            "\"user\":\"kimchy\"," +
            "\"postDate\":\"2013-01-30\"," +
            "\"message\":\"trying out Elasticsearch\"" +
            "}";

        IndexResponse response = client.prepareIndex("twitter", "tweet")
                    .setSource(json)
                    .get();
        
        
    }
    
    private void createUpdateBulk() throws Exception {
        Settings settings = Settings.settingsBuilder().put("cluster.name", "prodcat-dev").build();
        Client client = TransportClient.builder().settings(settings).build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        // either use client#prepare, or use Requests# to directly build index/delete requests
        bulkRequest.add(client.prepareIndex("twitter", "tweet", "1")
            .setSource(jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject()
            )
        );

        bulkRequest.add(client.prepareIndex("twitter", "tweet", "2")
            .setSource(jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "another post")
                .endObject()
            )
        );

        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }
    }
}
