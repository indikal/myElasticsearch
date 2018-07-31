package lk.indika.elasticsearch;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ProductSearchDTO implements Serializable {

    private static final long serialVersionUID = -7820445324764221458L;

    private String productId;
    private String productType;
    private String serviceType;
    private Date byTimeStamp;
    private String currency;
    private String customerId;
    private String marketId;
    private String subscriptionCode;
    private String claimType;
    private String operationsProvider;
    private String zoneId;
    private Boolean priceData = false;
    private String countryCode;
    private String province;
    private String city;
    private Long organizationDid;
    private List<Long> assortmentDids;
    private List<Long> productDids;
    private List<String> itemIds;

    private List<Long> agreementDids;
    private List<Long> commodityDids;

    /* Part of fields related findProduct ws method */
    private String query;
    private String sortParamName;
    private Boolean isParamAscending = false;
    private int offset;
    private int maxResults;
    private Boolean isAllowAnyRank = true;
    private Long navigationDid;
    private String navigationNodeId;
    private boolean rootNodeBased;

    private Boolean isOrderable = true;

    private Map filtering; //supplierId, supplierItemCode, eco, priceType

    private Long customerDid;
    private Long buyerDid;
    private Integer[] actuality;
    private Long[] authorizedWhoDids;
    
    private Long pricelistDid;
    private Long supplierDid;
    private Integer pricelistActuality;
    private String priceTypeCode;
    private String brand;
    private Date validityDateFrom;
    private Date validityDateUntil;
    private String supplierItemId;
    private String buyerItemId;
    private String itemDenominationm;
    private Boolean itemsWithOutBuyerItemCode;
    private Long categoryDid;
    private String quantityUnitCode;
    private Integer itemActionCode; //DML code
    private Long packageLevelDid;
    private Long packageTypeDid;
    private Long priceTypeDid;
    private Long itemsNotInCategoryDid;
    private Long itemsNotInAssortmentDid;
    private Boolean itemsWithOutCategory;
    private Boolean itemsWithOutAssortment;
    

    private boolean isHitCountsSearch; // This is only true for FindProducts4,FindProducts3 and FindProducts2 API methods
    private String comparatorSortField;

    {
        filtering = new HashMap();
    }

    public void addFilter(String fieldName, List values) {
        filtering.put(fieldName, values);
    }

    public Map getFiltering() {
        return filtering;
    }

    public Long getOrganizationDid() {
        return organizationDid;
    }

    public void setOrganizationDid(Long organizationDid) {
        this.organizationDid = organizationDid;
    }

    public List<Long> getAssortmentDids() {
        return assortmentDids;
    }

    public void setAssortmentDids(List<Long> assortmentDids) {
        this.assortmentDids = assortmentDids;
    }

    public List<Long> getProductDids() {
        return productDids;
    }

    public void setProductDids(List<Long> productDids) {
        this.productDids = productDids;
    }

    public List<String> getItemIds() {
		return itemIds;
	}

	public void setItemIds(List<String> itemIds) {
		this.itemIds = itemIds;
	}

	public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductType() {
        return productType;
    }

    public void setProductType(String productType) {
        this.productType = productType;
    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public Date getByTimeStamp() {
        return byTimeStamp;
    }

    public void setByTimeStamp(Date byTimeStamp) {
        this.byTimeStamp = byTimeStamp;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getMarketId() {
        return marketId;
    }

    public void setMarketId(String marketId) {
        this.marketId = marketId;
    }

    public String getSubscriptionCode() {
        return subscriptionCode;
    }

    public void setSubscriptionCode(String subscriptionCode) {
        this.subscriptionCode = subscriptionCode;
    }

    public String getClaimType() {
        return claimType;
    }

    public void setClaimType(String claimType) {
        this.claimType = claimType;
    }

    public String getOperationsProvider() {
        return operationsProvider;
    }

    public void setOperationsProvider(String operationsProvider) {
        this.operationsProvider = operationsProvider;
    }

    public String getZoneId() {
        return zoneId;
    }

    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }

    public Boolean getPriceData() {
        return priceData;
    }

    public void setPriceData(Boolean priceData) {
        this.priceData = priceData;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public List<Long> getAgreementDids() {
        return agreementDids;
    }

    public void setAgreementDids(List<Long> agreementDids) {
        this.agreementDids = agreementDids;
    }

    public List<Long> getCommodityDids() {
        return commodityDids;
    }

    public void setCommodityDids(List<Long> commodityDids) {
        this.commodityDids = commodityDids;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getSortParamName() {
        return sortParamName;
    }

    public void setSortParamName(String sortParamName) {
        this.sortParamName = sortParamName;
    }

    public Boolean getIsParamAscending() {
        return isParamAscending;
    }

    public void setIsParamAscending(Boolean isParamAscending) {
        this.isParamAscending = isParamAscending;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(int maxResults) {
        this.maxResults = maxResults;
    }

    public Boolean getIsAllowAnyRank() {
        return isAllowAnyRank;
    }

    public void setIsAllowAnyRank(Boolean isAllowAnyRank) {
        this.isAllowAnyRank = isAllowAnyRank;
    }

	public Long getNavigationDid() {
		return navigationDid;
	}

	public void setNavigationDid(Long navigationDid) {
		this.navigationDid = navigationDid;
	}

	public Boolean getIsOrderable() {
		return isOrderable;
	}

	public void setIsOrderable(Boolean isOrderable) {
		this.isOrderable = isOrderable;
	}

	public Long getCustomerDid() {
        return customerDid;
    }

    public void setCustomerDid(Long customerDid) {
        this.customerDid = customerDid;
    }

    public Long getBuyerDid() {
		return buyerDid;
	}

	public void setBuyerDid(Long buyerDid) {
		this.buyerDid = buyerDid;
	}

	public Integer[] getActuality() {
        return actuality;
    }

    public void setActuality(Integer[] actuality) {
        this.actuality = actuality;
    }

    public Long[] getAuthorizedWhoDids() {
        return authorizedWhoDids;
    }

    public void setAuthorizedWhoDids(Long[] authorizedWhoDids) {
        this.authorizedWhoDids = authorizedWhoDids;
    }

    public boolean isHitCountsSearch() {
        return isHitCountsSearch;
    }

    public void setHitCountsSearch(boolean isHitCountsSearch) {
        this.isHitCountsSearch = isHitCountsSearch;
    }

	public String getNavigationNodeId() {
		return navigationNodeId;
	}

	public void setNavigationNodeId(String navigationNodeId) {
		this.navigationNodeId = navigationNodeId;
	}

	public boolean isRootNodeBased() {
		return rootNodeBased;
	}

	public void setRootNodeBased(boolean rootNodeBased) {
		this.rootNodeBased = rootNodeBased;
	}

	public Long getPricelistDid() {
		return pricelistDid;
	}

	public void setPricelistDid(Long pricelistDid) {
		this.pricelistDid = pricelistDid;
	}

	public String getPriceTypeCode() {
		return priceTypeCode;
	}

	public void setPriceTypeCode(String priceTypeCode) {
		this.priceTypeCode = priceTypeCode;
	}

	public Long getSupplierDid() {
		return supplierDid;
	}

	public void setSupplierDid(Long supplierDid) {
		this.supplierDid = supplierDid;
	}

	public Integer getPricelistActuality() {
		return pricelistActuality;
	}

	public void setPricelistActuality(Integer pricelistActuality) {
		this.pricelistActuality = pricelistActuality;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public Date getValidityDateFrom() {
		return validityDateFrom;
	}

	public void setValidityDateFrom(Date validityDateFrom) {
		this.validityDateFrom = validityDateFrom;
	}

	public Date getValidityDateUntil() {
		return validityDateUntil;
	}

	public void setValidityDateUntil(Date validityDateUntil) {
		this.validityDateUntil = validityDateUntil;
	}

	public String getSupplierItemId() {
		return supplierItemId;
	}

	public void setSupplierItemId(String supplierItemId) {
		this.supplierItemId = supplierItemId;
	}

	public String getBuyerItemId() {
		return buyerItemId;
	}

	public void setBuyerItemId(String buyerItemId) {
		this.buyerItemId = buyerItemId;
	}

	public String getItemDenominationm() {
		return itemDenominationm;
	}

	public void setItemDenominationm(String itemDenominationm) {
		this.itemDenominationm = itemDenominationm;
	}

	public Boolean getItemsWithOutBuyerItemCode() {
		return itemsWithOutBuyerItemCode;
	}

	public void setItemsWithOutBuyerItemCode(Boolean itemsWithOutBuyerItemCode) {
		this.itemsWithOutBuyerItemCode = itemsWithOutBuyerItemCode;
	}

	public Long getCategoryDid() {
		return categoryDid;
	}

	public void setCategoryDid(Long categoryDid) {
		this.categoryDid = categoryDid;
	}

	public String getQuantityUnitCode() {
		return quantityUnitCode;
	}

	public void setQuantityUnitCode(String quantityUnitCode) {
		this.quantityUnitCode = quantityUnitCode;
	}

	public Integer getItemActionCode() {
		return itemActionCode;
	}

	public void setItemActionCode(Integer itemActionCode) {
		this.itemActionCode = itemActionCode;
	}

	public Long getPackageLevelDid() {
		return packageLevelDid;
	}

	public void setPackageLevelDid(Long packageLevelDid) {
		this.packageLevelDid = packageLevelDid;
	}

	public Long getPackageTypeDid() {
		return packageTypeDid;
	}

	public void setPackageTypeDid(Long packageTypeDid) {
		this.packageTypeDid = packageTypeDid;
	}

	public Long getPriceTypeDid() {
		return priceTypeDid;
	}

	public void setPriceTypeDid(Long priceTypeDid) {
		this.priceTypeDid = priceTypeDid;
	}

    public Long getItemsNotInCategoryDid() {
        return itemsNotInCategoryDid;
    }

    public void setItemsNotInCategoryDid(Long itemsNotInCategoryDid) {
        this.itemsNotInCategoryDid = itemsNotInCategoryDid;
    }

    public Long getItemsNotInAssortmentDid() {
        return itemsNotInAssortmentDid;
    }

    public void setItemsNotInAssortmentDid(Long itemsNotInAssortmentDid) {
        this.itemsNotInAssortmentDid = itemsNotInAssortmentDid;
    }

    public Boolean getItemsWithOutCategory() {
        return itemsWithOutCategory;
    }

    public void setItemsWithOutCategory(Boolean itemsWithOutCategory) {
        this.itemsWithOutCategory = itemsWithOutCategory;
    }

    public Boolean getItemsWithOutAssortment() {
        return itemsWithOutAssortment;
    }

    public void setItemsWithOutAssortment(Boolean itemsWithOutAssortment) {
        this.itemsWithOutAssortment = itemsWithOutAssortment;
    }

	public String getComparatorSortField() {
		return comparatorSortField;
	}

	public void setComparatorSortField(String comparatorSortField) {
		this.comparatorSortField = comparatorSortField;
	}
}