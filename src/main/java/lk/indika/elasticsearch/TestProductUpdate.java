/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
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
public class TestProductUpdate {
    public static void main(String[] args) {
        //updateAllEcoItems();
        //updateBuyerInfo();
        //updateAssortments();
        //updateAgreementInfo();
        updateSupplierCode();
        //escapeSpecialRegexChars();
    }
    
    public static void updateAssortments() {
        Map<String, Object> fieldValues = new HashMap<String, Object>();
		fieldValues.put("assortments", "[assortments]");
        
        ProductSearchDTO searchDTO = new ProductSearchDTO();
        searchDTO.setIsOrderable(Boolean.FALSE);
		//searchDTO.setSupplierItemId("577*");
        searchDTO.setAssortmentDids(new ArrayList<>(Arrays.asList(2433510L)));
        
        updateProductChanges(searchDTO, fieldValues, "localhost");
    }
    
    public static void updateAllEcoItems() {
        Map<String, Object> fieldValues = new HashMap<String, Object>();
		fieldValues.put("buyerprodcode", "ES_[supplierprodcode]");
        
        ProductSearchDTO searchDTO = new ProductSearchDTO();
        searchDTO.setIsOrderable(Boolean.FALSE);
		searchDTO.setPricelistDid(2492981L);
		searchDTO.setSupplierDid(201982L);
		searchDTO.setPricelistActuality(2);
		searchDTO.setSupplierItemId("577338D");
        
        updateProductChanges(searchDTO, fieldValues, "localhost");
    }
    
    public static void updateSupplierCode() {
        Map<String, Object> fieldValues = new HashMap<String, Object>();
		//fieldValues.put("buyerprodcode", "ES2_[supplierprodcode]");
        fieldValues.put("prodname", "Salto {}()[]:\".+*?~^!$|/\\ klar 35cl 6");
        fieldValues.put("_prodname", "Salto {}()[]:\".+*?~^!$|/\\ klar 35cl 6");
        
        List<Long> productDids = new ArrayList<Long>();
        ProductSearchDTO searchDTO = new ProductSearchDTO();
        //searchDTO.setIsOrderable(Boolean.FALSE);
        productDids.add(134900L);
		searchDTO.setProductDids(productDids);
        
        updateProductChanges(searchDTO, fieldValues, "localhost");
    }
    
    public static void updateBuyerInfo() {
        Map<String, Object> fieldValues = new HashMap<String, Object>();
		//fieldValues.put("buyerprodcode", "ES2_[supplierprodcode]");
        fieldValues.put("prodname", "Journalmapp svart jan feb mars LiV");
        fieldValues.put("prodnamefreetext", "Journalmapp svart jan feb mars LiV");
        
        List<Long> productDids = new ArrayList<Long>();
        ProductSearchDTO searchDTO = new ProductSearchDTO();
        searchDTO.setIsOrderable(Boolean.FALSE);
        productDids.add(134900L);
		searchDTO.setProductDids(productDids);
        
        updateProductChanges(searchDTO, fieldValues, "localhost");
    }
    
    public static void updateAllBuyerInfo() {
        Map<String, Object> fieldValues = new HashMap<String, Object>();
		fieldValues.put("buyerprodcode", "ES_[supplierprodcode]");
        
        ProductSearchDTO searchDTO = new ProductSearchDTO();
        searchDTO.setIsOrderable(Boolean.FALSE);
		searchDTO.setPricelistDid(2492981L);
		searchDTO.setSupplierDid(201982L);
		searchDTO.setPricelistActuality(2);
		searchDTO.setSupplierItemId("577338D");
        
        updateProductChanges(searchDTO, fieldValues, "localhost");
    }
	
	public static void updateAgreementInfo() {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        ProductSearchDTO searchDTO = new ProductSearchDTO();
		Map<String, Object> fieldValues = new HashMap<String, Object>();
		
		searchDTO.setIsOrderable(Boolean.FALSE);
		searchDTO.setAgreementDids(Arrays.asList(2115193L));
        searchDTO.setCustomerDid(26131L);
		
		fieldValues.put("agreementstatusdid", 2L);
		fieldValues.put("agreementfromdate", "2015-04-18");
		fieldValues.put("agreementuntildate", "2018-12-31");
		fieldValues.put("supplieragreementno", "BOHUSS001");
		fieldValues.put("isfreightcostallowed", 0);
		fieldValues.put("isexpressdeliveryallowed", 0);
		fieldValues.put("isscheduleddelivery", 0);
		fieldValues.put("isautoattestinvoiceallowed", 0);
		
        List<Map<String, Object>> agreementGroups = new ArrayList<Map<String, Object>>();
           
        Map<String, Object> group = new HashMap<String, Object>();
        group.put("buyerdid", 29244L);
        group.put("isbuyerdisabled", 1);
        group.put("partyshipvalidfrom", null);
        group.put("partyshipvaliduntil", "2018-12-31");
        agreementGroups.add(group);
        
        group = new HashMap<String, Object>();
        group.put("buyerdid", 29320L);
        group.put("isbuyerdisabled", 0);
        group.put("partyshipvalidfrom", null);
        group.put("partyshipvaliduntil", "2018-12-31");
        agreementGroups.add(group);
        
        group = new HashMap<String, Object>();
        group.put("buyerdid", 29321L);
        group.put("isbuyerdisabled", 0);
        group.put("partyshipvalidfrom", null);
        group.put("partyshipvaliduntil", "2018-12-31");
        agreementGroups.add(group);
        
        group = new HashMap<String, Object>();
        group.put("buyerdid", 29322L);
        group.put("isbuyerdisabled", 0);
        group.put("partyshipvalidfrom", null);
        group.put("partyshipvaliduntil", "2018-12-31");
        agreementGroups.add(group);
        
        fieldValues.put("agreementgroups", agreementGroups);
        
		System.out.println("Updating agreement changes: " + 2115193);
		updateProductChanges(searchDTO, fieldValues, "localhost");
	}
    
    public static void updateProductChanges(ProductSearchDTO searchDTO, Map<String, Object> fieldValues, String tenant) {
        try {
        	long start = System.currentTimeMillis();
            long total_docs = 0;
            ElasticsearchClientFactory factory = ElasticsearchClientFactory.getInstance();

    		BoolQueryBuilder query = ElasticHelper.getFindProductsQuery(searchDTO);
    		
            BulkProcessor bulkProcessor = factory.getBulkProcessor();
            //now get all documents matching the search
            //1000 hits per shard will be returned for each scroll
            SearchResponse scrollResp = factory.prepareSearch("ehav-qa")
                    .setTypes("products")
                    .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                    .setScroll(new TimeValue(60000))
                    .setQuery(query)
                    .setSize(1000).execute().actionGet();

            if (null != scrollResp && null != scrollResp.getHits() 
                && null != scrollResp.getHits().getHits() && scrollResp.getHits().getHits().length > 0) {
                //Scroll until no hits are returned
                while (true) {

                    for (SearchHit hit : scrollResp.getHits().getHits()) {
                        total_docs++;
                        //Handle the hit...
                        Map<String, Object> fields = hit.getSource();
                        String buyerProdCode = (String) fieldValues.get("buyerprodcode");
                        if (null != buyerProdCode) {
                            System.out.println("Updating buyer prod code: " + buyerProdCode);
                        	fieldValues.put("buyerprodcode", 
                        			buyerProdCode.replace("[supplierprodcode]", fields.get("supplierprodcode").toString()));
                        }
                        
                        String assortments = fields.get("assortments").toString();
                        assortments = assortments.replaceAll("\\|\\|", "");
                        fieldValues.put("assortments", assortments);
    				
                        String id = fields.get("documentid").toString();
                        if (null != id) {
                            //System.out.println("Updating ducumentid: " + id + " " + fieldValues.get("agreementgroups").toString());
                            bulkProcessor.add(factory.prepareUpdateRequest("ehav-qa", id)
                                .setDoc(fieldValues)
                                .request()
                            );
                            //bulkProcessor.add(new DeleteRequest("twitter", "tweet", "2"));
                        }
                        
                    }
                    scrollResp = factory.prepareSearchScroll(scrollResp.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute()
                        .actionGet();
                    
                    //Break condition: No hits are returned
                    if (scrollResp.getHits().getHits().length == 0) {
                        break;
                    }
                }
    			
                //bulkProcessor.awaitClose(100, TimeUnit.MILLISECONDS);
                bulkProcessor.close();
                bulkProcessor = null;
                scrollResp = null;
                System.out.println("Elasticsearch index synchronized with " + total_docs + " products.");
    			
    		} else {
                System.out.println("No documents found in elasticsearch to be updated!");
            }
            System.out.println("====================> Time taken to sync data to Elasticsearch: " + (System.currentTimeMillis() - start) + "ms");
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
	
	private static ProductSearchDTO getProductSearchDTO() {
		ProductSearchDTO searchDTO = new ProductSearchDTO();

//		searchDTO.setPricelistDid(pliSearchDTO.getPricelistDIDs()[0]);
//		searchDTO.setSupplierDid(pliSearchDTO.getSelectedSupplierDIDs()[0]);
//		searchDTO.setActuality(new Integer[] { pliSearchDTO.getSelectedPriceListActuality() });
//		searchDTO.setBrand(pliSearchDTO.getBrand());
//		searchDTO.setValidityDateFrom(pliSearchDTO.getPriceValidFrom());
//		searchDTO.setValidityDateUntil(pliSearchDTO.getPriceValidTo());
//		searchDTO.setBuyerItemId(pliSearchDTO.getBuyerItemId());
//		searchDTO.setSupplierItemId(pliSearchDTO.getSupplierItemId());
//		searchDTO.setItemsWithOutBuyerItemCode(pliSearchDTO.getItemsWithoutBuyerItemId());
//		searchDTO.setCategoryDid(pliSearchDTO.getSelectedProductCategories()[0]);
//		searchDTO.setItemDenominationm(pliSearchDTO.getItemDenomination());
//		searchDTO.setQuantityUnitCode(getUnitCode(pliSearchDTO.getSelectedQuantityUnit()));
//		searchDTO.setItemActionCode(pliSearchDTO.getSelectedItemActionCodeDID());
//		searchDTO.setPackageLevelDid(pliSearchDTO.getSelectedPackagingLevel());
//		searchDTO.setPackageTypeDid(pliSearchDTO.getSelectedPackageType());
//		searchDTO.setPriceTypeCode((pliSearchDTO.getSelectedPriceTypeCodeDID() == Constants.PRICE_TYPE_CODE_CATALOGUE_PRICE) ? "CT" : "CA");
//		
		return searchDTO;
	}
	
//	private static String getMetaCharEscaped(String value) {
//        String metaChar = "+ - && || ! ( ) { } [ ] ^ \" ~ * ? : \\";
//        String[] metaChars = metaChar.split(" ");
//        String escaped = value.replaceAll("[\\<\\(\\[\\{\\\\\\^\\-\\=\\$\\!\\|\\]\\}\\)‌​\\?\\*\\+\\.\\>]", "\\\\$0");
//        for (String meta : metaChars) {
//            
//        }
//        return null;
//    }
    
    private static void escapeSpecialRegexChars() {
        String str = "Asas\\Sa{}()[]:\".+*?~^!$|/\\";
        //Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[{}()\\[\\]:\".+*?~^!$\\\\|/]");
        
        String SPECIAL_CHARS = "{}()[]:\".+*?~^!$|/\\";
        String SYSTEM_CHARS = null;
        
        if (null == SYSTEM_CHARS || SYSTEM_CHARS.trim().length() == 0) {
            SYSTEM_CHARS = SPECIAL_CHARS;
        }
        
        SYSTEM_CHARS = SYSTEM_CHARS.replaceAll("\\\\", "\\\\\\\\");
        SYSTEM_CHARS = SYSTEM_CHARS.replaceAll("\\[", "\\\\[");
        SYSTEM_CHARS = SYSTEM_CHARS.replaceAll("\\]", "\\\\]");
        Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[" + SYSTEM_CHARS + "]");
        
        System.out.println("Original: " + str + ", Escapped: " + SPECIAL_REGEX_CHARS.matcher(str).replaceAll("\\\\$0"));
    }
}
