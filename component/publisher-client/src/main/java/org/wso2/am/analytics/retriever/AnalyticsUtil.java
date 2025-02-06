package org.wso2.am.analytics.retriever;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.wso2.am.analytics.retriever.choreo.ChoreoAnalyticsConstants;
import org.wso2.carbon.apimgt.api.APIManagementException;
import org.wso2.carbon.apimgt.api.APIProvider;
import org.wso2.carbon.apimgt.api.model.API;
import org.wso2.carbon.apimgt.api.model.APIProduct;
import org.wso2.carbon.apimgt.common.analytics.exceptions.AnalyticsException;
import org.wso2.carbon.apimgt.impl.APIConstants;
import org.wso2.carbon.apimgt.impl.APIManagerConfiguration;
import org.wso2.carbon.apimgt.impl.APIManagerFactory;
import org.wso2.carbon.apimgt.impl.utils.APIUtil;
import org.wso2.carbon.apimgt.persistence.APIPersistence;
import org.wso2.carbon.apimgt.persistence.PersistenceManager;
import org.wso2.carbon.apimgt.persistence.dto.Organization;
import org.wso2.carbon.apimgt.persistence.dto.PublisherAPI;
import org.wso2.carbon.apimgt.persistence.dto.PublisherAPIProduct;
import org.wso2.carbon.apimgt.persistence.exceptions.APIPersistenceException;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.user.api.Tenant;
import org.wso2.carbon.user.api.UserStoreException;
import org.wso2.carbon.utils.multitenancy.MultitenantConstants;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.wso2.am.analytics.retriever.choreo.ChoreoAnalyticsConstants.PRODUCTS;

public class AnalyticsUtil {

    APIPersistence apiPersistenceInstance;
    private static final Log log = LogFactory.getLog(AnalyticsUtil.class);

    /**
     * Returns the list of monetized API Ids with their tenants
     *
     * @return List<JSONArray>
     * @throws AnalyticsException if the action failed
     */
    public List<JSONArray> getMonetizedAPIIdsAndTenantDomains() throws AnalyticsException {

        JSONArray monetizedAPIIdsList = new JSONArray();
        JSONArray tenantDomainList = new JSONArray();
        List<JSONArray> tenantsAndApis = new ArrayList<>(2);
        try {
            Properties properties = new Properties();
            properties.put(APIConstants.ALLOW_MULTIPLE_STATUS, APIUtil.isAllowDisplayAPIsWithMultipleStatus());
            properties.put(APIConstants.ALLOW_MULTIPLE_VERSIONS, APIUtil.isAllowDisplayMultipleVersions());
            Map<String, String> configMap = new HashMap<>();
            Map<String, String> configs = APIManagerConfiguration.getPersistenceProperties();
            if (configs != null && !configs.isEmpty()) {
                configMap.putAll(configs);
            }
            configMap.put(APIConstants.ALLOW_MULTIPLE_STATUS,
                    Boolean.toString(APIUtil.isAllowDisplayAPIsWithMultipleStatus()));

            apiPersistenceInstance = PersistenceManager.getPersistenceInstance(configMap, properties);
            List<Tenant> tenants = APIUtil.getAllTenantsWithSuperTenant();
            for (Tenant tenant : tenants) {
                tenantDomainList.add(tenant.getDomain());
                try {
                    PrivilegedCarbonContext.startTenantFlow();
                    PrivilegedCarbonContext.getThreadLocalCarbonContext().setTenantDomain(
                            tenant.getDomain(), true);
                    String tenantAdminUsername = APIUtil.getAdminUsername();
                    if (!MultitenantConstants.SUPER_TENANT_DOMAIN_NAME.equals(tenant.getDomain())) {
                        tenantAdminUsername =
                                APIUtil.getAdminUsername() + ChoreoAnalyticsConstants.AT + tenant.getDomain();
                    }
                    APIProvider apiProviderNew = APIManagerFactory.getInstance().getAPIProvider(tenantAdminUsername);
                    List<API> allowedAPIs = apiProviderNew.getAllAPIs();
                    Organization org = new Organization(tenant.getDomain());
                    for (API api : allowedAPIs) {
                        PublisherAPI publisherAPI = null;
                        try {
                            publisherAPI = apiPersistenceInstance.getPublisherAPI(org, api.getUUID());
                            if (publisherAPI.isMonetizationEnabled()) {
                                monetizedAPIIdsList.add(api.getUUID());
                            }
                        } catch (APIPersistenceException e) {
                            throw new AnalyticsException("Failed to retrieve the API of UUID: " + api.getUUID(), e);
                        }
                    }
                    Map<String, Object> productMap = apiProviderNew.searchPaginatedAPIProducts("", tenant.getDomain(), 0,
                            Integer.MAX_VALUE);
                    if (productMap != null && productMap.containsKey(PRODUCTS)) {
                        SortedSet<APIProduct> productSet = (SortedSet<APIProduct>) productMap.get(PRODUCTS);
                        for (APIProduct apiProduct : productSet) {
                            PublisherAPIProduct publisherAPIProduct;
                            try {
                                publisherAPIProduct = apiPersistenceInstance.getPublisherAPIProduct(org,
                                        apiProduct.getUuid());
                                if (publisherAPIProduct.isMonetizationEnabled()) {
                                    monetizedAPIIdsList.add(apiProduct.getUuid());
                                }
                            } catch (APIPersistenceException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                } catch (APIManagementException e) {
                    throw new AnalyticsException("Error while retrieving the Ids of Monetized APIs");
                }
            }
        } catch (UserStoreException e) {
            throw new AnalyticsException("Error while retrieving the tenants", e);
        }
        tenantsAndApis.add(tenantDomainList);
        tenantsAndApis.add(monetizedAPIIdsList);
        return tenantsAndApis;
    }

    /**
     * The method converts the date into timestamp
     *
     * @param date
     * @return Timestamp in long format
     */
    public long getTimestamp(String date) {

        SimpleDateFormat formatter = new SimpleDateFormat(ChoreoAnalyticsConstants.TIME_FORMAT);
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        long time = 0;
        Date parsedDate = null;
        try {
            parsedDate = formatter.parse(date);
            time = parsedDate.getTime();
        } catch (java.text.ParseException e) {
            log.error("Error while parsing the date ", e);
        }
        return time;
    }

}
