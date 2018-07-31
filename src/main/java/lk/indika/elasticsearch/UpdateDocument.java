package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class UpdateDocument {

	public static void main(String[] args) {
		try {
			String itemDid = "108076";
			// set the cluster name if you use one different than "elasticsearch":
			Settings settings = Settings.settingsBuilder().put("cluster.name", "prodcat_inli-dev").build();
			Client client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			
			UpdateRequest updateRequest = new UpdateRequest("prodcat", "item", itemDid)
			        .doc(jsonBuilder()
			            .startObject()
			                .field("narcoticscode", "ANZ")
			                .field("assortmentid", "1234567")
			            .endObject());
			client.update(updateRequest).get();
			System.out.println("Updated the document with ID: " + itemDid);
			
			//get the updated item to test
			GetResponse get = client.prepareGet("prodcat", "item", itemDid).get();
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

}
