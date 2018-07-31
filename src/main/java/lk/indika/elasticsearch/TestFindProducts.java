package lk.indika.elasticsearch;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.LocalDate;

public class TestFindProducts {

    public static void main(String[] args) {
        try {
            //set the cluster name if you use one different than "elasticsearch":
            Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "prodcat-dev").build();
            Client client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

            long start = System.currentTimeMillis();
            int page_no = 0;
            String sortFieldName = "tradeitemdid";
            SortOrder sortOrder = SortOrder.DESC;

            //ProductSearchDTO searchDTO = getSearchDTO_findBySupplierData();
            ProductSearchDTO searchDTO = getSearchDTO_findProducts3();
            
            System.out.println("Creating Elasticsearch query ...");
            BoolQueryBuilder query;
            query = getFindProductsQuery(searchDTO);

            System.out.println("Executing Elasticsearch query ...");
            SearchResponse response = client.prepareSearch("ehav-test")
                .setTypes("products")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(query)
                .addSort(sortFieldName, sortOrder).addSort("tradeitemdid", SortOrder.DESC)
                .setFrom((searchDTO.getOffset() <= 0) ? 0 : searchDTO.getOffset())
                .setSize((searchDTO.getMaxResults() <= 0) ? 20 : searchDTO.getMaxResults())
                .setExplain(false)
                .execute()
                .actionGet();
            System.out.println("Executed the Elasticsearch query ...");

            SearchHits shits = response.getHits();
            long total_recs = shits.getTotalHits();
            int i = page_no;
            System.out.println("Total Records: " + total_recs + "\n");

//            for (SearchHit hit : shits) {
//                System.out.println("Record " + ++i + " of " + total_recs + " [Product Did: " + hit.getId() + "]");
//
//                Map<String, Object> fields = hit.getSource();
//                for (String key : fields.keySet()) {
//                    System.out.println(key + ": " + fields.get(key));
//                }
//                System.out.println("\n----------------------------------------------------------\n ");
//            }
            System.out.println("Total time taken: " + (System.currentTimeMillis() - start) + "ms");

            // on shutdown
            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    private static BoolQueryBuilder getFindProductsQuery(ProductSearchDTO searchDTO) {
        System.out.println("Creating Elasticsearch query ...");
        int max_rec_count = searchDTO.getMaxResults();
        
        BoolQueryBuilder query = QueryBuilders.boolQuery();

        System.out.println("Adding customer did to Elasticsearch query: " + searchDTO.getCustomerDid());
        query.must(QueryBuilders.termQuery("customerdid", searchDTO.getCustomerDid()));

        System.out.println("Adding buyer did to Elasticsearch query: " + searchDTO.getBuyerDid());
        query.must(QueryBuilders.termQuery("buyerdid", searchDTO.getBuyerDid()));

        if (null != searchDTO.getItemIds() && searchDTO.getItemIds().size() > 0) {
            System.out.println("Adding item IDs string to Elasticsearch query ...");
            BoolQueryBuilder itemIds = QueryBuilders.boolQuery();
            itemIds.minimumNumberShouldMatch(1);

            for (String id : searchDTO.getItemIds()) {
                itemIds.should(QueryBuilders.queryStringQuery(searchDTO.getQuery())
                    .field("supplierprodcode")
                    .field("buyerprodcode"));
            }
            max_rec_count = (searchDTO.getItemIds().size() > max_rec_count) ? searchDTO.getItemIds().size() : max_rec_count;
            query.must(itemIds);
        }
        if (StringUtils.isNotEmpty(searchDTO.getCommodityDids())) {
            //Commodity DIDs
            System.out.println("Adding commodity dids to Elasticsearch query ...");
            BoolQueryBuilder commodityDids = QueryBuilders.boolQuery();
            commodityDids.minimumNumberShouldMatch(1);

            for (Long did : searchDTO.getCommodityDids()) {
                commodityDids.should(QueryBuilders.termQuery("commoditydid", did));
            }
            max_rec_count = (searchDTO.getCommodityDids().size() > max_rec_count) ? searchDTO.getCommodityDids().size() : max_rec_count;
            query.must(commodityDids);
        }
        
        //checks for orderable items
        if (StringUtils.isNotEmpty(searchDTO.getIsOrderable())) {
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
            query.must(agreementfromdate); //null OR < sysdate
            
            BoolQueryBuilder agreementuntildate = QueryBuilders.boolQuery();
            agreementuntildate.minimumNumberShouldMatch(1);
            agreementuntildate.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("agreementuntildate")));
            agreementuntildate.should(QueryBuilders.rangeQuery("agreementuntildate").format("yyyy-MM-dd").gte(curdate));
            query.must(agreementuntildate); //null OR > sysdate

            BoolQueryBuilder pricelistactivefrom = QueryBuilders.boolQuery();
            pricelistactivefrom.minimumNumberShouldMatch(1);
            pricelistactivefrom.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("pricelistactivefrom")));
            pricelistactivefrom.should(QueryBuilders.rangeQuery("pricelistactivefrom").format("yyyy-MM-dd").lte(curdate));
            query.must(pricelistactivefrom); //null OR < sysdate
            
            BoolQueryBuilder pricelistactiveuntil = QueryBuilders.boolQuery();
            pricelistactiveuntil.minimumNumberShouldMatch(1);
            pricelistactiveuntil.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("pricelistactiveuntil")));
            pricelistactiveuntil.should(QueryBuilders.rangeQuery("pricelistactiveuntil").format("yyyy-MM-dd").gte(curdate));
            query.must(pricelistactiveuntil); //null OR > sysdate

            BoolQueryBuilder partyshipvalidfrom = QueryBuilders.boolQuery();
            partyshipvalidfrom.minimumNumberShouldMatch(1);
            partyshipvalidfrom.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("partyshipvalidfrom")));
            partyshipvalidfrom.should(QueryBuilders.rangeQuery("partyshipvalidfrom").format("yyyy-MM-dd").lte(curdate));
            query.must(partyshipvalidfrom); //null OR < sysdate
            
            BoolQueryBuilder partyshipvaliduntil = QueryBuilders.boolQuery();
            partyshipvaliduntil.minimumNumberShouldMatch(1);
            partyshipvaliduntil.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("partyshipvaliduntil")));
            partyshipvaliduntil.should(QueryBuilders.rangeQuery("partyshipvaliduntil").format("yyyy-MM-dd").gte(curdate));
            query.must(partyshipvaliduntil); //null OR > sysdate

            query.must(QueryBuilders.termQuery("issupplierdisabled", 0));
            query.must(QueryBuilders.termQuery("isbuyerdisabled", 0));
        }

    	if (StringUtils.isNotEmpty(searchDTO.getAgreementDids())) {
            //AgreementDIDs
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
            regex = ".*(" + regex + ").*";
            query.must(QueryBuilders.regexpQuery("assortmentdids", regex));
        }
        if (StringUtils.isNotEmpty(searchDTO.getAuthorizedWhoDids())) {
            String regex = org.apache.commons.lang.StringUtils.join(searchDTO.getAuthorizedWhoDids(), "|");
            System.out.println("Adding authorized dids to Elasticsearch query: " + regex);
            regex = ".*(" + regex + ").*";
            query.must(QueryBuilders.regexpQuery("authorizerdids", regex));
        }
        if (StringUtils.isNotEmpty(searchDTO.getProductDids())) {
            //TRADEITEMDID
            System.out.println("Adding tradeitem dids to Elasticsearch query ...");
            BoolQueryBuilder tradeitemDids = QueryBuilders.boolQuery();
            tradeitemDids.minimumNumberShouldMatch(1);

            for (Long did : searchDTO.getProductDids()) {
                tradeitemDids.should(QueryBuilders.termQuery("tradeitemdid", did));
            }
            max_rec_count = (searchDTO.getProductDids().size() > max_rec_count) ? searchDTO.getProductDids().size() : max_rec_count;
            query.must(tradeitemDids);
        }
        if (searchDTO.getFiltering() != null) {
            System.out.println("Adding filters to Elasticsearch query ...");
            if (searchDTO.getFiltering().containsKey(Constants.WS_FILER_PARAM_SUPPLIER_ID)) {
                //supplierDids
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
                //SUPPLIERITEMID
                if (StringUtils.isNotEmpty(searchDTO.getFiltering().get(Constants.WS_FILTER_PARAM_SUPPLIER_PROD_CODE))) {
                    System.out.println("Adding supplier item ids to Elasticsearch query ...");
                    BoolQueryBuilder supProductCodes = QueryBuilders.boolQuery();
                    List<String> supProductCodeList = (List<String>) searchDTO.getFiltering().get(Constants.WS_FILTER_PARAM_SUPPLIER_PROD_CODE);
                    supProductCodes.minimumNumberShouldMatch(1);
                    
                    for (String code : supProductCodeList) {
                        supProductCodes.should(QueryBuilders.queryStringQuery(code).field("supplierprodcode"));
                    }
                    max_rec_count = (supProductCodeList.size() > max_rec_count) ? supProductCodeList.size() : max_rec_count;
                    query.must(supProductCodes);
                }
            }
            if (searchDTO.getFiltering().containsKey(Constants.WS_FILER_PARAM_PRICE_TYPE)) {
                //pricetypecode
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
                //eco true=1, false=0
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
            //either in BUYERITEMID, SUPPLIERITEMID, SUPPLIERNAME, DENOMINATION, COMMODITYAGREEMENTREF
            System.out.println("Adding query string to Elasticsearch query ...");
            query.must(QueryBuilders.queryStringQuery("*" + searchDTO.getQuery() + "*")
                .field("prodname")
                .field("supplierprodcode")
                .field("buyerprodcode")
                .field("supplieragreementno"));
        }
        if (searchDTO.getNavigationDid() != null) {
            System.out.println("Adding navigation did to Elasticsearch query ...");
            query.must(QueryBuilders.queryStringQuery(searchDTO.getNavigationDid().toString())
                .field("navigationnodepath"));
        }
    	if (searchDTO.getIsAllowAnyRank() != null && !searchDTO.getIsAllowAnyRank().booleanValue()) {
            BoolQueryBuilder anyrank = QueryBuilders.boolQuery();
            anyrank.minimumNumberShouldMatch(1);
            anyrank.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("rankvalue")));
            anyrank.should(QueryBuilders.termQuery("rankvalue", "1"));
            query.must(anyrank);
    	}
        
    	searchDTO.setMaxResults(max_rec_count);
        System.out.println(query.toString());
        return query;
    }
    
    private static ProductSearchDTO getSearchDTO_findBySupplierData() {
        ProductSearchDTO searchDto = new ProductSearchDTO();
        ArrayList supplierDids = new ArrayList();
        ArrayList supProdCodes = new ArrayList();
        
        supplierDids.add("199500");
        supProdCodes.add("3000");
        
        searchDto.setCustomerDid(26131L);
        searchDto.setBuyerDid(29244L);
        searchDto.setAuthorizedWhoDids(new Long[]{29497L,29206L,29425L,29412L,29244L,26131L});
        searchDto.addFilter(Constants.WS_FILER_PARAM_SUPPLIER_ID, supplierDids);
        searchDto.addFilter(Constants.WS_FILTER_PARAM_SUPPLIER_PROD_CODE, supProdCodes);
        
        return searchDto;
    }
    
    private static ProductSearchDTO getSearchDTO_findProducts3() {
        ProductSearchDTO searchDto = new ProductSearchDTO();
        ArrayList assortmentDids = new ArrayList();
        ArrayList priceTypes = new ArrayList();
        ArrayList eco = new ArrayList();
        
        assortmentDids.add(2433512L);
        
        searchDto.setCustomerDid(26131L);
        searchDto.setBuyerDid(29244L);
        searchDto.setAssortmentDids(assortmentDids);
        searchDto.setAuthorizedWhoDids(new Long[]{29497L,29206L,29425L,29412L,29244L,26131L});
        searchDto.setIsAllowAnyRank(Boolean.TRUE);
        
        searchDto.setSortParamName("category");
        searchDto.setIsParamAscending(Boolean.TRUE);
        
        searchDto.setOffset(0);
        searchDto.setMaxResults(20);
        return searchDto;
    }
    
    private static ProductSearchDTO getSearchDTO_findProducts4() {
        ProductSearchDTO searchDto = new ProductSearchDTO();
        ArrayList assortmentDids = new ArrayList();
        ArrayList priceTypes = new ArrayList();
        ArrayList eco = new ArrayList();
        
        assortmentDids.add(2433512L);
        priceTypes.add("CT");
        priceTypes.add("CA");
        eco.add("true");
        
        searchDto.setCustomerDid(26131L);
        searchDto.setBuyerDid(29244L);
        searchDto.setAssortmentDids(assortmentDids);
        searchDto.setAuthorizedWhoDids(new Long[]{29497L,29206L,29425L,29412L,29244L,26131L});
        searchDto.addFilter(Constants.WS_FILER_PARAM_PRICE_TYPE, priceTypes);
        searchDto.addFilter(Constants.WS_FILER_PARAM_ECO, eco);
        
        searchDto.setSortParamName("category");
        searchDto.setIsParamAscending(Boolean.TRUE);
        
        searchDto.setOffset(0);
        searchDto.setMaxResults(20);
        return searchDto;
    }
    
    private static ProductSearchDTO getSearchDTO() {
        ProductSearchDTO searchDto = new ProductSearchDTO();
        ArrayList assortmentDids = new ArrayList();
        ArrayList supplierDids = new ArrayList();
        ArrayList priceTypes = new ArrayList();
        ArrayList eco = new ArrayList();
        ArrayList supProdCodes = new ArrayList();
        
        assortmentDids.add(2433512L);
        assortmentDids.add(4344234L);
        supplierDids.add("198523");
        //priceTypes.add("CT");
        priceTypes.add("CA");
        eco.add("true");
        supProdCodes.add("710401");
        
        searchDto.setAssortmentDids(assortmentDids);
        searchDto.setAuthorizedWhoDids(new Long[]{29497L,29206L,29425L,29412L,29244L,26131L});
        searchDto.setNavigationNodeId("2391381");
        searchDto.setBuyerDid(29244L);
        searchDto.addFilter(Constants.WS_FILER_PARAM_SUPPLIER_ID, supplierDids);
        searchDto.addFilter(Constants.WS_FILER_PARAM_PRICE_TYPE, priceTypes);
        searchDto.addFilter(Constants.WS_FILER_PARAM_ECO, eco);
        //searchDto.addFilter(Constants.WS_FILTER_PARAM_SUPPLIER_PROD_CODE, supProdCodes);
        
        searchDto.setSortParamName("category");
        searchDto.setIsParamAscending(Boolean.TRUE);
        
        searchDto.setOffset(0);
        searchDto.setMaxResults(20);
        return searchDto;
    }
}
