/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lk.indika.elasticsearch;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.LocalDate;

/**
 *
 * @author indika
 */
public class ElasticHelper {
    public static BoolQueryBuilder getFindProductsQuery(ProductSearchDTO searchDTO) {
		System.out.println("Creating Elasticsearch query ...");
		int max_rec_count = searchDTO.getMaxResults();

		BoolQueryBuilder query = QueryBuilders.boolQuery();

		if (null != searchDTO.getCustomerDid()) {
			System.out.println("Adding customer did to Elasticsearch query: " + searchDTO.getCustomerDid());
			query.must(QueryBuilders.termQuery("customerdid", searchDTO.getCustomerDid()));
		}

		if (null != searchDTO.getBuyerDid()) {
			System.out.println("Adding buyer did to Elasticsearch query: " + searchDTO.getBuyerDid());
			query.must(QueryBuilders.nestedQuery("agreementgroups", QueryBuilders.boolQuery().must(
                    QueryBuilders.termQuery("agreementgroups.buyerdid", searchDTO.getBuyerDid()))));
		}

		if (null != searchDTO.getItemIds() && searchDTO.getItemIds().size() > 0) {
			System.out.println("Adding item IDs string to Elasticsearch query ...");
			BoolQueryBuilder itemIds = QueryBuilders.boolQuery();
			itemIds.minimumNumberShouldMatch(1);

			for (String id : searchDTO.getItemIds()) {
				itemIds.should(QueryBuilders.queryStringQuery(id).field("supplierprodcode").field("buyerprodcode"));
			}
			query.must(itemIds);
		}
		if (StringUtils.isNotEmpty(searchDTO.getCommodityDids())) {
			// Commodity DIDs
			System.out.println("Adding commodity dids to Elasticsearch query ...");
			BoolQueryBuilder commodityDids = QueryBuilders.boolQuery();
			commodityDids.minimumNumberShouldMatch(1);

			for (Long did : searchDTO.getCommodityDids()) {
				commodityDids.should(QueryBuilders.termQuery("commoditydid", did));
			}
			max_rec_count = (searchDTO.getCommodityDids().size() > max_rec_count) ? searchDTO.getCommodityDids().size() : max_rec_count;
			query.must(commodityDids);
		}

		// checks for orderable items
		if (StringUtils.isNotEmpty(searchDTO.getIsOrderable()) && searchDTO.getIsOrderable()) {
            setOrderableFilters(query);
		}

		if (StringUtils.isNotEmpty(searchDTO.getAgreementDids())) {
			// AgreementDIDs
			System.out.println("Adding agreement dids to Elasticsearch query ...");
			BoolQueryBuilder agreementDids = QueryBuilders.boolQuery();
			agreementDids.minimumNumberShouldMatch(1);

			for (Long did : searchDTO.getAgreementDids()) {
				agreementDids.should(QueryBuilders.termQuery("agreementdid", did));
			}
			query.must(agreementDids);
		}
		if (StringUtils.isNotEmpty(searchDTO.getAssortmentDids())) {
			String regex = org.apache.commons.lang.StringUtils.join(searchDTO.getAssortmentDids().iterator(), "|");
			System.out.println("Adding assortment dids to Elasticsearch query: " + regex);
			regex = "(^?|.*\\|)(" + regex + "):.*";
			query.must(QueryBuilders.regexpQuery("assortments", regex));
		}
		
		/*if (StringUtils.isNotEmpty(searchDTO.getAuthorizedWhoDids())) {
			String regex = org.apache.commons.lang.StringUtils.join(searchDTO.getAuthorizedWhoDids(), "|"); 
			System.out.println("Adding authorized dids to Elasticsearch query: " + regex); regex =
					".*(" + regex + ").*";
			query.must(QueryBuilders.regexpQuery("authorizerdids", regex)); 
		}*/
		 
		if (StringUtils.isNotEmpty(searchDTO.getProductDids())) {
			// TRADEITEMDID
			System.out.println("Adding tradeitem dids to Elasticsearch query ...");
			BoolQueryBuilder tradeitemDids = QueryBuilders.boolQuery();
			tradeitemDids.minimumNumberShouldMatch(1);

			for (Long did : searchDTO.getProductDids()) {
				tradeitemDids.should(QueryBuilders.termQuery("tradeitemdid", did));
			}
			max_rec_count = (searchDTO.getProductDids().size() > max_rec_count) ? searchDTO.getProductDids().size() : max_rec_count;
			query.must(tradeitemDids);
		}
		if (searchDTO.getFiltering() != null && searchDTO.getFiltering().size() > 0) {
			System.out.println("Adding filters to Elasticsearch query ...");
			if (searchDTO.getFiltering().containsKey(Constants.WS_FILER_PARAM_SUPPLIER_ID)) {
				// supplierDids
				if (StringUtils.isNotEmpty(searchDTO.getFiltering().get(Constants.WS_FILER_PARAM_SUPPLIER_ID))) {
					System.out.println("Adding supplier dids to Elasticsearch query ...");
					BoolQueryBuilder supplierDids = QueryBuilders.boolQuery();
					List<String> supplierList = (List<String>) searchDTO.getFiltering().get(Constants.WS_FILER_PARAM_SUPPLIER_ID);
					supplierDids.minimumNumberShouldMatch(1);

					for (String did : supplierList) {
						supplierDids.should(QueryBuilders.termQuery("supplierid", did));
					}
					query.must(supplierDids);
				}
			}
			if (searchDTO.getFiltering().containsKey(Constants.WS_FILTER_PARAM_SUPPLIER_PROD_CODE)) {
				// SUPPLIERITEMID
				if (StringUtils.isNotEmpty(searchDTO.getFiltering().get(Constants.WS_FILTER_PARAM_SUPPLIER_PROD_CODE))) {
					System.out.println("Adding supplier item ids to Elasticsearch query ...");
					BoolQueryBuilder supProductCodes = QueryBuilders.boolQuery();
					List<String> supProductCodeList = (List<String>) searchDTO.getFiltering().get(Constants.WS_FILTER_PARAM_SUPPLIER_PROD_CODE);
					supProductCodes.minimumNumberShouldMatch(1);

					for (String code : supProductCodeList) {
						supProductCodes.should(QueryBuilders.queryStringQuery(code).field("supplierprodcode"));
					}
					query.must(supProductCodes);
				}
			}
			if (searchDTO.getFiltering().containsKey(Constants.WS_FILER_PARAM_PRICE_TYPE)) {
				// pricetypecode
				if (StringUtils.isNotEmpty(searchDTO.getFiltering().get(Constants.WS_FILER_PARAM_PRICE_TYPE))) {
					System.out.println("Adding price type codes to Elasticsearch query ...");
					BoolQueryBuilder priceTypes = QueryBuilders.boolQuery();
					List<String> priceTypeList = (List<String>) searchDTO.getFiltering().get(Constants.WS_FILER_PARAM_PRICE_TYPE);
					priceTypes.minimumNumberShouldMatch(1);

					for (String priceType : priceTypeList) {
						priceTypes.should(QueryBuilders.queryStringQuery(priceType).field("pricetype"));
					}
					query.must(priceTypes);
				}
			}
			if (searchDTO.getFiltering().containsKey(Constants.WS_FILER_PARAM_ECO)) {
				// eco true=1, false=0
				if (StringUtils.isNotEmpty(searchDTO.getFiltering().get(Constants.WS_FILER_PARAM_ECO))) {
					System.out.println("Adding eco to Elasticsearch query ...");
					List<String> ecoList = (List<String>) searchDTO.getFiltering().get(Constants.WS_FILER_PARAM_ECO);

					String eco = ecoList.get(0);
					if (eco != null && eco.equalsIgnoreCase("true")) {
						query.must(QueryBuilders.termQuery("eco", "1"));
					} else if (eco != null && eco.equalsIgnoreCase("false")) {
						query.must(QueryBuilders.termQuery("eco", "0"));
					}
				}
			}
		}
		if (StringUtils.isNotEmpty(searchDTO.getQuery())) {
			// either in BUYERITEMID, SUPPLIERITEMID, SUPPLIERNAME,
			// DENOMINATION, COMMODITYAGREEMENTREF
            BoolQueryBuilder freeTestQuery = QueryBuilders.boolQuery();
			String searchQuery = searchDTO.getQuery().trim();
            searchQuery = escapeSpecialRegexChars(searchQuery);
//            searchQuery = searchQuery.replaceAll("\\\\", "\\\\\\\\");
//            searchQuery = searchQuery.replaceAll("\\(", "\\\\(");
//            searchQuery = searchQuery.replaceAll("\\)", "\\\\)");
//            searchQuery = searchQuery.replaceAll("/", "\\\\/");
//            searchQuery = searchQuery.replaceAll("\\.", "\\\\.");
//            searchQuery = searchQuery.replaceAll("\\*", "\\\\*");
			String free_text = "*" + searchQuery.replaceAll(" +", "* AND *") + "*";
            
			System.out.println("Adding free text query as analyzed to Elasticsearch: " + free_text);
			freeTestQuery.should(QueryBuilders.queryStringQuery(free_text)
	                .field("_prodname").field("_supplierprodcode").field("_buyerprodcode")
						.field("_supplieragreementno").field("_eanid").field("_suppliername")
							.field("_atccode").field("_atcdenomination"));
			
			//free_text = free_text.replaceAll(".", "\\.");
			free_text = "*" + searchQuery.replaceAll(" +", "*") + "*";
            System.out.println("Adding free text query as not_analyzed to Elasticsearch: " + free_text);
			freeTestQuery.should(QueryBuilders.wildcardQuery("prodname", free_text));
			freeTestQuery.should(QueryBuilders.wildcardQuery("supplierprodcode", free_text));
			freeTestQuery.should(QueryBuilders.wildcardQuery("buyerprodcode", free_text));
			freeTestQuery.should(QueryBuilders.wildcardQuery("supplieragreementno", free_text));
			freeTestQuery.should(QueryBuilders.wildcardQuery("eanid", free_text));
			freeTestQuery.should(QueryBuilders.wildcardQuery("suppliername", free_text));
			freeTestQuery.should(QueryBuilders.wildcardQuery("atccode", free_text));
			freeTestQuery.should(QueryBuilders.wildcardQuery("atcdenomination", free_text));
//	                .field("prodname").field("supplierprodcode").field("buyerprodcode")
//						.field("supplieragreementno").field("eanid").field("suppliername")
//							.field("atccode").field("atcdenomination"));
			
            query.must(freeTestQuery);
//			String free_text = "*" + searchDTO.getQuery().trim().replaceAll(" +", "* AND *") + "*";
//            free_text = free_text.replaceAll("/", "//");
//            
//			System.out.println("Adding free text query to Elasticsearch: " + free_text);
//            query.must(QueryBuilders.queryStringQuery(free_text)
//                .field("_prodname").field("_supplierprodcode").field("_buyerprodcode")
//					.field("_supplieragreementno").field("_eanid").field("_suppliername"));
            
//            BoolQueryBuilder byquery = QueryBuilders.boolQuery();
//			byquery.minimumNumberShouldMatch(1);
//			byquery.should(QueryBuilders.wildcardQuery("prodname", "*" + searchDTO.getQuery() + "*"));
//			byquery.should(QueryBuilders.wildcardQuery("supplierprodcode", "*" + searchDTO.getQuery() + "*"));
//			byquery.should(QueryBuilders.wildcardQuery("buyerprodcode", "*" + searchDTO.getQuery() + "*"));
//			byquery.should(QueryBuilders.wildcardQuery("supplieragreementno", "*" + searchDTO.getQuery() + "*"));
//			byquery.should(QueryBuilders.wildcardQuery("eanid", "*" + searchDTO.getQuery() + "*"));
//			query.must(byquery);
		}
		if (searchDTO.getNavigationDid() != null) {
			System.out.println("Adding navigation did to Elasticsearch query ...");
			query.must(QueryBuilders.queryStringQuery(
                "*" + searchDTO.getNavigationDid().toString() + "*").field("navigationnodepath"));
		}
		if (searchDTO.getIsAllowAnyRank() != null && !searchDTO.getIsAllowAnyRank().booleanValue()) {
			BoolQueryBuilder anyrank = QueryBuilders.boolQuery();
			anyrank.minimumNumberShouldMatch(1);
			anyrank.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("rankvalue")));
			anyrank.should(QueryBuilders.termQuery("rankvalue", "1"));
			query.must(anyrank);
		}
		
		//extra search parameters for product synchronization part
		if (null != searchDTO.getPricelistDid() && searchDTO.getPricelistDid() > 0) {
			System.out.println("Adding pricelist did to Elasticsearch query: " + searchDTO.getPricelistDid());
			query.must(QueryBuilders.termQuery("pricelistdid", searchDTO.getPricelistDid()));
		}
		
		if (null != searchDTO.getSupplierDid() && searchDTO.getSupplierDid() > 0) {
			System.out.println("Adding supplier did to Elasticsearch query: " + searchDTO.getSupplierDid());
			query.must(QueryBuilders.termQuery("supplierid", searchDTO.getSupplierDid()));
		}
		
		//if PL actuality is NEW that item is not in ES. Handle it out of ES query
		if (null != searchDTO.getPricelistActuality() && searchDTO.getPricelistActuality() > 0) {
			System.out.println("Adding pricelist actuality to Elasticsearch query: " + searchDTO.getPricelistActuality());
			query.must(QueryBuilders.termQuery("pricelistactuality", searchDTO.getPricelistActuality()));
		}
		
		if (null != searchDTO.getPriceTypeCode()) {
			System.out.println("Adding price type code to Elasticsearch query: " + searchDTO.getPriceTypeCode());
			query.must(QueryBuilders.termQuery("pricetype", searchDTO.getPriceTypeCode()));
		}
		
		if (null != searchDTO.getBrand()) {
			System.out.println("Adding brand to Elasticsearch query: " + searchDTO.getBrand());
			query.must(QueryBuilders.wildcardQuery("brand", searchDTO.getBrand()));
		}
		
		if (null != searchDTO.getValidityDateFrom()) {
			LocalDate fromdate = new LocalDate(searchDTO.getValidityDateFrom());
			BoolQueryBuilder pricelistactivefrom = QueryBuilders.boolQuery();
			pricelistactivefrom.minimumNumberShouldMatch(1);
			pricelistactivefrom.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("pricelistactivefrom")));
			pricelistactivefrom.should(QueryBuilders.rangeQuery("pricelistactivefrom").format("yyyy-MM-dd").lte(fromdate));
			query.must(pricelistactivefrom);
		}

		if (null != searchDTO.getValidityDateUntil()) {
			LocalDate untildate = new LocalDate(searchDTO.getValidityDateUntil());
			BoolQueryBuilder pricelistactiveuntil = QueryBuilders.boolQuery();
			pricelistactiveuntil.minimumNumberShouldMatch(1);
			pricelistactiveuntil.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("pricelistactiveuntil")));
			pricelistactiveuntil.should(QueryBuilders.rangeQuery("pricelistactiveuntil").format("yyyy-MM-dd").gte(untildate));
			query.must(pricelistactiveuntil);
		}
		
		if (StringUtils.isNotEmpty(searchDTO.getSupplierItemId())) {
			System.out.println("Adding supplier item id to Elasticsearch query: " + searchDTO.getSupplierItemId());
			query.must(QueryBuilders.wildcardQuery("supplierprodcode", searchDTO.getSupplierItemId()));
		}
		
		if (StringUtils.isNotEmpty(searchDTO.getBuyerItemId())) {
			System.out.println("Adding buyer item id to Elasticsearch query ...");
			query.must(QueryBuilders.wildcardQuery("buyerprodcode", searchDTO.getBuyerItemId()));
		}
		
		if (StringUtils.isNotEmpty(searchDTO.getItemDenominationm())) {
			System.out.println("Adding item denomination to Elasticsearch query ...");
			query.must(QueryBuilders.wildcardQuery("prodname", searchDTO.getItemDenominationm()));
		}
		
		if (null != searchDTO.getItemsWithOutBuyerItemCode() && searchDTO.getItemsWithOutBuyerItemCode()) {
			query.mustNot(QueryBuilders.existsQuery("buyerprodcode"));
		}
		
		if (null != searchDTO.getCategoryDid() && searchDTO.getCategoryDid() > 0) {
			System.out.println("Adding category did to Elasticsearch query ...");
			query.must(QueryBuilders.termQuery("navigationnodeid", searchDTO.getCategoryDid()));
		}
		
		if (null != searchDTO.getQuantityUnitCode()) {
			System.out.println("Adding price unit to Elasticsearch query: " + searchDTO.getQuantityUnitCode());
			query.must(QueryBuilders.termQuery("priceunit", searchDTO.getQuantityUnitCode()));
		}
		
		if (null != searchDTO.getItemActionCode() && searchDTO.getItemActionCode() >= 1 && searchDTO.getItemActionCode() <= 3) {
			System.out.println("Adding dml code to Elasticsearch query: " + searchDTO.getItemActionCode());
			String dmlcode = null;
			switch (searchDTO.getItemActionCode()) {
			case 1:
				dmlcode = "I";
				break;
			case 2:
				dmlcode = "D";
				break;
			case 3:
				dmlcode = "U";
				break;
			default:
				break;
			}
			query.must(QueryBuilders.termQuery("dmltype", dmlcode));
		}
		
		if (null != searchDTO.getPackageLevelDid()) {
			System.out.println("Adding package level to Elasticsearch query: " + searchDTO.getPackageLevelDid());
			query.must(QueryBuilders.termQuery("pkgleveldid", searchDTO.getPackageLevelDid()));
		}
		
		if (null != searchDTO.getPackageTypeDid()) {
			System.out.println("Adding package type to Elasticsearch query: " + searchDTO.getPackageTypeDid());
			query.must(QueryBuilders.termQuery("pkgtypedid", searchDTO.getPackageTypeDid()));
		}
		
		if (null != searchDTO.getPriceTypeDid()) {
			System.out.println("Adding price type to Elasticsearch query: " + searchDTO.getPriceTypeDid());
			query.must(QueryBuilders.termQuery("pricetype", (searchDTO.getPriceTypeDid() == 1) ? "CA" : "CT"));
		}
        
		if (StringUtils.isNotEmpty(searchDTO.getItemsNotInAssortmentDid())) {
			System.out.println("Adding items not in assortment did to Elasticsearch query: " + searchDTO.getItemsNotInAssortmentDid());
			String regex = "(^?|.*\\|)(" + searchDTO.getItemsNotInAssortmentDid() + "):.*";
			query.must(QueryBuilders.existsQuery("assortments"));
			query.mustNot(QueryBuilders.regexpQuery("assortments", regex));
		}
		
		if (null != searchDTO.getItemsNotInCategoryDid() && searchDTO.getItemsNotInCategoryDid() > 0) {
			System.out.println("Adding items not in category did to Elasticsearch query ...");
			query.must(QueryBuilders.existsQuery("navigationnodeid"));
            query.mustNot(QueryBuilders.termQuery("navigationnodeid", searchDTO.getCategoryDid()));
		}
        
		if (StringUtils.isNotEmpty(searchDTO.getItemsWithOutAssortment())) {
			System.out.println("Adding items with out assortment to Elasticsearch query");
			BoolQueryBuilder withOutAssortment = QueryBuilders.boolQuery();
			withOutAssortment.minimumNumberShouldMatch(1);
			withOutAssortment.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("assortments")));
			withOutAssortment.should(QueryBuilders.termQuery("assortments", "null"));
			query.must(withOutAssortment);
		}
        
		if (StringUtils.isNotEmpty(searchDTO.getItemsWithOutCategory())) {
			System.out.println("Adding items with out category to Elasticsearch query");
			BoolQueryBuilder withOutCategory = QueryBuilders.boolQuery();
			withOutCategory.minimumNumberShouldMatch(1);
			withOutCategory.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("navigationnodeid")));
			withOutCategory.should(QueryBuilders.termQuery("navigationnodeid", "null"));
			query.must(withOutCategory);
		}
		
		searchDTO.setMaxResults(max_rec_count);
		return query;
	}

    private static String escapeSpecialRegexChars(String str) {
        //String str = "Sa*l.to/";
        //Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[{}()\\[\\].+*?^$\\|/]");
        
        //return SPECIAL_REGEX_CHARS.matcher(str).replaceAll("\\\\$0");
        
        String SPECIAL_CHARS = "{}()[]:\".+*?~^!$\\|/";
        String SYSTEM_CHARS = null;
        
        if (null == SYSTEM_CHARS || SYSTEM_CHARS.trim().length() == 0) {
            SYSTEM_CHARS = SPECIAL_CHARS;
        }
        
        SYSTEM_CHARS = SYSTEM_CHARS.replaceAll("\\\\", "\\\\\\\\");
        SYSTEM_CHARS = SYSTEM_CHARS.replaceAll("\\[", "\\\\[");
        SYSTEM_CHARS = SYSTEM_CHARS.replaceAll("\\]", "\\\\]");
        Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[" + SYSTEM_CHARS + "]");

        return SPECIAL_REGEX_CHARS.matcher(str).replaceAll("\\\\$0");
    }
    
    private static void setOrderableFilters(BoolQueryBuilder query) {
        query.must(QueryBuilders.termQuery("pricelistactuality", 2));
        query.must(QueryBuilders.termQuery("commodityactuality", 2));
        query.must(QueryBuilders.termQuery("tradeitemactuality", 2));
        query.must(QueryBuilders.termQuery("priceactuality", 2));
        query.must(QueryBuilders.termQuery("agreementstatusdid", 2));
        query.must(QueryBuilders.termQuery("isorderable", 1));

        LocalDate curdate = new LocalDate();
        BoolQueryBuilder agreementfromdate = QueryBuilders.boolQuery();
        agreementfromdate.minimumNumberShouldMatch(1);
        agreementfromdate.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("agreementfromdate")));
        agreementfromdate.should(QueryBuilders.rangeQuery("agreementfromdate").format("yyyy-MM-dd").lte(curdate));
        query.must(agreementfromdate); // null OR < sysdate

        BoolQueryBuilder agreementuntildate = QueryBuilders.boolQuery();
        agreementuntildate.minimumNumberShouldMatch(1);
        agreementuntildate.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("agreementuntildate")));
        agreementuntildate.should(QueryBuilders.rangeQuery("agreementuntildate").format("yyyy-MM-dd").gte(curdate));
        query.must(agreementuntildate); // null OR > sysdate

        BoolQueryBuilder pricelistactivefrom = QueryBuilders.boolQuery();
        pricelistactivefrom.minimumNumberShouldMatch(1);
        pricelistactivefrom.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("pricelistactivefrom")));
        pricelistactivefrom.should(QueryBuilders.rangeQuery("pricelistactivefrom").format("yyyy-MM-dd").lte(curdate));
        query.must(pricelistactivefrom); // null OR < sysdate

        BoolQueryBuilder pricelistactiveuntil = QueryBuilders.boolQuery();
        pricelistactiveuntil.minimumNumberShouldMatch(1);
        pricelistactiveuntil.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("pricelistactiveuntil")));
        pricelistactiveuntil.should(QueryBuilders.rangeQuery("pricelistactiveuntil").format("yyyy-MM-dd").gte(curdate));
        query.must(pricelistactiveuntil); // null OR > sysdate

        BoolQueryBuilder partyshipvalidfrom = QueryBuilders.boolQuery();
        partyshipvalidfrom.minimumNumberShouldMatch(1);
        partyshipvalidfrom.should(QueryBuilders.nestedQuery("agreementgroups", QueryBuilders.boolQuery().mustNot(
            QueryBuilders.existsQuery("agreementgroups.partyshipvalidfrom"))));
        partyshipvalidfrom.should(QueryBuilders.nestedQuery("agreementgroups", QueryBuilders.boolQuery().must(
            QueryBuilders.rangeQuery("agreementgroups.partyshipvalidfrom").format("yyyy-MM-dd").lte(curdate))));
        query.must(partyshipvalidfrom); // null OR < sysdate

        BoolQueryBuilder partyshipvaliduntil = QueryBuilders.boolQuery();
        partyshipvaliduntil.minimumNumberShouldMatch(1);
        partyshipvaliduntil.should(QueryBuilders.nestedQuery("agreementgroups", QueryBuilders.boolQuery().mustNot(
            QueryBuilders.existsQuery("agreementgroups.partyshipvaliduntil"))));
        partyshipvaliduntil.should(QueryBuilders.nestedQuery("agreementgroups", QueryBuilders.boolQuery().must(
            QueryBuilders.rangeQuery("agreementgroups.partyshipvaliduntil").format("yyyy-MM-dd").gte(curdate))));
        query.must(partyshipvaliduntil); // null OR > sysdate

        query.must(QueryBuilders.termQuery("issupplierdisabled", 0));
        query.must(QueryBuilders.nestedQuery("agreementgroups", QueryBuilders.boolQuery().must(
            QueryBuilders.termQuery("agreementgroups.isbuyerdisabled", 0))));
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
