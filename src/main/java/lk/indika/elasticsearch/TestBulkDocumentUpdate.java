/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 *
 * @author indika
 */
public class TestBulkDocumentUpdate {
    public static void main(String[] args) {
        syncProductChanges();
    }
    
    private static void syncProductChanges() {
        try {
            List<Object[]> products = new ArrayList();
            products.add(new String[]{"134598292441", "{ \"documentid\":13459829244,\"tradeitemdid\":134598,\"allowedqtydecimals\":null,\"buyerprodcode\":\"MENK502107D\",\"prodname\":\"Lock PC trans 1/1 6\",\"costaccount\":\"\",\"pricevariance\":0,\"replacesitemdid\":\"\",\"commoditydid\":2887530,\"supplierprodcode\":\"502107D\",\"ismedical\":0,\"leadtimedays\":null,\"supplierdid\":31442,\"infourl\":\"\",\"imageurl\":\"/ebprodcat/service/image?tObjectDid=2887530&sourceName=502107D\",\"eco\":1,\"brand\":\"Cambro \",\"isorderable\":1,\"agreementdid\":2115187,\"supplieragreementno\":\"TANUM200711\",\"customerdid\":26131,\"agreementstatusdid\":2,\"agreementfromdate\":\"2005-02-09\",\"agreementuntildate\":\"2017-01-31\",\"isfreightcostallowed\":0,\"isexpressdeliveryallowed\":0,\"isscheduleddelivery\":0,\"isautoattestinvoiceallowed\":0,\"ranklistid\":\"\",\"rankvalue\":\"\",\"supplierpricelistno\":\"711525\",\"pricelistdid\":2492981,\"pricelistactivefrom\":\"2016-07-25\",\"pricelistactiveuntil\":\"\",\"validitystartdate\":\"2016-07-25\",\"price\":94.16,\"pricecompare\":0,\"pricetype\":\"CA\",\"priceunit\":\"PCE\",\"currency\":\"SEK\",\"vatrate\":25,\"minorderqty\":1,\"qtystep\":null,\"packqty\":1000,\"entityclassdid\":1,\"narcoticscode\":\"\",\"buyerdid\":29244,\"partyshipvalidfrom\":\"2016-06-03\",\"partyshipvaliduntil\":\"\",\"supplierid\":\"201982\",\"tenant\":\"localhost\",\"suppliername\":\"MENIGO AB\",\"issupplierdisabled\":0,\"isbuyerdisabled\":0,\"producttype\":\"V\",\"navigationnodepath\":\"2391381/2394264/2392197/2392991\",\"category\":\"Rotnod/Maskiner och utrustning och materiel för servicenäringen/Storköksutrustning/Utrustning för kokning och varmhållning\",\"navigationnodeid\":\"2392991\",\"assortmentdids\":\"2433510,2433511,2433512,2433513\",\"authorizerdids\":\"29320,29321,29244,29322\",\"proditemdescription\":\"Quantity in text=1000,,Smallest package=1,\",\"pricelistactuality\":2,\"commodityactuality\":2,\"tradeitemactuality\":2,\"priceactuality\":2,\"lastchangedate\":\"2016-08-16T17:05:55.000Z\"}"}); 
            products.add(new String[]{"134598293211", "{ \"documentid\":13459829321,\"tradeitemdid\":134598,\"allowedqtydecimals\":null,\"buyerprodcode\":\"MENK502107D\",\"prodname\":\"Lock PC trans 1/1 6\",\"costaccount\":\"\",\"pricevariance\":0,\"replacesitemdid\":\"\",\"commoditydid\":2887530,\"supplierprodcode\":\"502107D\",\"ismedical\":0,\"leadtimedays\":null,\"supplierdid\":31442,\"infourl\":\"\",\"imageurl\":\"/ebprodcat/service/image?tObjectDid=2887530&sourceName=502107D\",\"eco\":1,\"brand\":\"Cambro \",\"isorderable\":1,\"agreementdid\":2115187,\"supplieragreementno\":\"TANUM200711\",\"customerdid\":26131,\"agreementstatusdid\":2,\"agreementfromdate\":\"2005-02-09\",\"agreementuntildate\":\"2017-01-31\",\"isfreightcostallowed\":0,\"isexpressdeliveryallowed\":0,\"isscheduleddelivery\":0,\"isautoattestinvoiceallowed\":0,\"ranklistid\":\"\",\"rankvalue\":\"\",\"supplierpricelistno\":\"711525\",\"pricelistdid\":2492981,\"pricelistactivefrom\":\"2016-07-25\",\"pricelistactiveuntil\":\"\",\"validitystartdate\":\"2016-07-25\",\"price\":94.16,\"pricecompare\":0,\"pricetype\":\"CA\",\"priceunit\":\"PCE\",\"currency\":\"SEK\",\"vatrate\":25,\"minorderqty\":1,\"qtystep\":null,\"packqty\":1000,\"entityclassdid\":1,\"narcoticscode\":\"\",\"buyerdid\":29321,\"partyshipvalidfrom\":\"2016-06-03\",\"partyshipvaliduntil\":\"\",\"supplierid\":\"201982\",\"tenant\":\"localhost\",\"suppliername\":\"MENIGO AB\",\"issupplierdisabled\":0,\"isbuyerdisabled\":0,\"producttype\":\"V\",\"navigationnodepath\":\"2391381/2394264/2392197/2392991\",\"category\":\"Rotnod/Maskiner och utrustning och materiel för servicenäringen/Storköksutrustning/Utrustning för kokning och varmhållning\",\"navigationnodeid\":\"2392991\",\"assortmentdids\":\"2433510,2433511,2433512,2433513\",\"authorizerdids\":\"29320,29321,29244,29322\",\"proditemdescription\":\"Quantity in text=1000,,Smallest package=1,\",\"pricelistactuality\":2,\"commodityactuality\":2,\"tradeitemactuality\":2,\"priceactuality\":2,\"lastchangedate\":\"2016-08-16T17:05:55.000Z\"}"});
            products.add(new String[]{"134598293201", "{ \"documentid\":13459829320,\"tradeitemdid\":134598,\"allowedqtydecimals\":null,\"buyerprodcode\":\"MENK502107D\",\"prodname\":\"Lock PC trans 1/1 6\",\"costaccount\":\"\",\"pricevariance\":0,\"replacesitemdid\":\"\",\"commoditydid\":2887530,\"supplierprodcode\":\"502107D\",\"ismedical\":0,\"leadtimedays\":null,\"supplierdid\":31442,\"infourl\":\"\",\"imageurl\":\"/ebprodcat/service/image?tObjectDid=2887530&sourceName=502107D\",\"eco\":1,\"brand\":\"Cambro \",\"isorderable\":1,\"agreementdid\":2115187,\"supplieragreementno\":\"TANUM200711\",\"customerdid\":26131,\"agreementstatusdid\":2,\"agreementfromdate\":\"2005-02-09\",\"agreementuntildate\":\"2017-01-31\",\"isfreightcostallowed\":0,\"isexpressdeliveryallowed\":0,\"isscheduleddelivery\":0,\"isautoattestinvoiceallowed\":0,\"ranklistid\":\"\",\"rankvalue\":\"\",\"supplierpricelistno\":\"711525\",\"pricelistdid\":2492981,\"pricelistactivefrom\":\"2016-07-25\",\"pricelistactiveuntil\":\"\",\"validitystartdate\":\"2016-07-25\",\"price\":94.16,\"pricecompare\":0,\"pricetype\":\"CA\",\"priceunit\":\"PCE\",\"currency\":\"SEK\",\"vatrate\":25,\"minorderqty\":1,\"qtystep\":null,\"packqty\":1000,\"entityclassdid\":1,\"narcoticscode\":\"\",\"buyerdid\":29320,\"partyshipvalidfrom\":\"2016-06-03\",\"partyshipvaliduntil\":\"\",\"supplierid\":\"201982\",\"tenant\":\"localhost\",\"suppliername\":\"MENIGO AB\",\"issupplierdisabled\":0,\"isbuyerdisabled\":0,\"producttype\":\"V\",\"navigationnodepath\":\"2391381/2394264/2392197/2392991\",\"category\":\"Rotnod/Maskiner och utrustning och materiel för servicenäringen/Storköksutrustning/Utrustning för kokning och varmhållning\",\"navigationnodeid\":\"2392991\",\"assortmentdids\":\"2433510,2433511,2433512,2433513\",\"authorizerdids\":\"29320,29321,29244,29322\",\"proditemdescription\":\"Quantity in text=1000,,Smallest package=1,\",\"pricelistactuality\":2,\"commodityactuality\":2,\"tradeitemactuality\":2,\"priceactuality\":2,\"lastchangedate\":\"2016-08-16T17:05:55.000Z\"}"}); 
            products.add(new String[]{"134598293221", "{ \"documentid\":13459829322,\"tradeitemdid\":134598,\"allowedqtydecimals\":null,\"buyerprodcode\":\"MENK502107D\",\"prodname\":\"Lock PC trans 1/1 6\",\"costaccount\":\"\",\"pricevariance\":0,\"replacesitemdid\":\"\",\"commoditydid\":2887530,\"supplierprodcode\":\"502107D\",\"ismedical\":0,\"leadtimedays\":null,\"supplierdid\":31442,\"infourl\":\"\",\"imageurl\":\"/ebprodcat/service/image?tObjectDid=2887530&sourceName=502107D\",\"eco\":1,\"brand\":\"Cambro \",\"isorderable\":1,\"agreementdid\":2115187,\"supplieragreementno\":\"TANUM200711\",\"customerdid\":26131,\"agreementstatusdid\":2,\"agreementfromdate\":\"2005-02-09\",\"agreementuntildate\":\"2017-01-31\",\"isfreightcostallowed\":0,\"isexpressdeliveryallowed\":0,\"isscheduleddelivery\":0,\"isautoattestinvoiceallowed\":0,\"ranklistid\":\"\",\"rankvalue\":\"\",\"supplierpricelistno\":\"711525\",\"pricelistdid\":2492981,\"pricelistactivefrom\":\"2016-07-25\",\"pricelistactiveuntil\":\"\",\"validitystartdate\":\"2016-07-25\",\"price\":94.16,\"pricecompare\":0,\"pricetype\":\"CA\",\"priceunit\":\"PCE\",\"currency\":\"SEK\",\"vatrate\":25,\"minorderqty\":1,\"qtystep\":null,\"packqty\":1000,\"entityclassdid\":1,\"narcoticscode\":\"\",\"buyerdid\":29322,\"partyshipvalidfrom\":\"2016-06-03\",\"partyshipvaliduntil\":\"\",\"supplierid\":\"201982\",\"tenant\":\"localhost\",\"suppliername\":\"MENIGO AB\",\"issupplierdisabled\":0,\"isbuyerdisabled\":0,\"producttype\":\"V\",\"navigationnodepath\":\"2391381/2394264/2392197/2392991\",\"category\":\"Rotnod/Maskiner och utrustning och materiel för servicenäringen/Storköksutrustning/Utrustning för kokning och varmhållning\",\"navigationnodeid\":\"2392991\",\"assortmentdids\":\"2433510,2433511,2433512,2433513\",\"authorizerdids\":\"29320,29321,29244,29322\",\"proditemdescription\":\"Quantity in text=1000,,Smallest package=1,\",\"pricelistactuality\":2,\"commodityactuality\":2,\"tradeitemactuality\":2,\"priceactuality\":2,\"lastchangedate\":\"2016-08-16T17:05:55.000Z\"}"});
//            products.add(new String[]{"13459829320", "{ \"eco\":1 }"}); 
            
            if (null != products && products.size() > 0) {
                Settings settings = Settings.settingsBuilder().put("cluster.name", "prodcat-dev").build();
                Client client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
			
                BulkRequestBuilder bulkRequest = client.prepareBulk();

                // either use client#prepare, or use Requests# to directly build index/delete requests
                for (Object[] row : products) {
                    Long id = Long.valueOf((String) row[0]);
                    String json = (String) row[1];
                    json = json.replaceAll("\"\"", "null");
                    
                    if (null != id && null != json && json.trim().length() > 0) {
                        System.out.println("Updating ducumentid: " + id);
                        bulkRequest.add(client.prepareIndex("ehav-dev", "products", id.toString())
                            .setSource(json)
                        );
                    }
                }

                BulkResponse bulkResponse = bulkRequest.get();
                if (bulkResponse.hasFailures()) {
                    // process failures by iterating through each bulk response item
                    System.out.println("Error: synchronizing product changes");
                } else {
                    System.out.println("Elasticsearch index synchronized with " + products.size() + " products.");
                }
            } else {
                System.out.println("No products found to update elasticsearch index ...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
