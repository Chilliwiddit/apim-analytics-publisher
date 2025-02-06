package org.wso2.am.analytics.retriever.choreo;

import com.google.gson.internal.LinkedTreeMap;
import feign.Feign;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import feign.okhttp.OkHttpClient;
import feign.slf4j.Slf4jLogger;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.wso2.am.analytics.retriever.AnalyticsConstants;
import org.wso2.am.analytics.retriever.AnalyticsUtil;
import org.wso2.am.analytics.retriever.choreo.model.GraphQLClient;
import org.wso2.am.analytics.retriever.choreo.model.GraphqlQueryModel;
import org.wso2.am.analytics.retriever.choreo.model.graphQLResponseClient;
import org.wso2.am.analytics.retriever.choreo.model.QueryAPIAccessTokenInterceptor;
import org.wso2.carbon.apimgt.api.APIAdmin;
import org.wso2.carbon.apimgt.api.APIManagementException;
import org.wso2.carbon.apimgt.api.APIProvider;
import org.wso2.carbon.apimgt.api.MonetizationException;
import org.wso2.carbon.apimgt.api.model.*;
import org.wso2.carbon.apimgt.common.analytics.exceptions.AnalyticsException;
import org.wso2.carbon.apimgt.impl.APIAdminImpl;
import org.wso2.carbon.apimgt.impl.APIConstants;
import org.wso2.carbon.apimgt.impl.APIManagerConfiguration;
import org.wso2.carbon.apimgt.impl.APIManagerFactory;
import org.wso2.carbon.apimgt.impl.dao.ApiMgtDAO;
import org.wso2.carbon.apimgt.impl.internal.MonetizationDataHolder;
import org.wso2.carbon.apimgt.impl.internal.ServiceReferenceHolder;
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

import static org.wso2.am.analytics.retriever.choreo.ChoreoAnalyticsConstants.*;


public class ChoreoAnalyticsforMonetizationImpl implements AnalyticsforMonetization {

    private static final Log log = LogFactory.getLog(ChoreoAnalyticsforMonetizationImpl.class);
    private static APIManagerConfiguration config = null;
    boolean useNewQueryAPI = true;
    AnalyticsUtil analyticsUtil = new AnalyticsUtil();

    /**
     * Gets Usage Data from Analytics Provider
     *
     * @param lastPublishInfo monetization publish info
     * @return usage data from analytics provider
     * @throws AnalyticsException if the action failed
     */
    @Override
    public Object getUsageData(MonetizationUsagePublishInfo lastPublishInfo) throws AnalyticsException {
        Long currentTimestamp;
        String apiUuid = null;
        String tenantDomain = null;
        String applicationName = null;
        String applicationOwner = null;
        Long requestCount = 0L;
        APIAdmin apiAdmin = new APIAdminImpl();

        Date dateobj = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(ChoreoAnalyticsConstants.TIME_FORMAT);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(ChoreoAnalyticsConstants.TIME_ZONE));
        String toDate = simpleDateFormat.format(dateobj);

        if (config == null) {
            // Retrieve the access token from api manager configurations.
            config = ServiceReferenceHolder.getInstance().getAPIManagerConfigurationService().
                    getAPIManagerConfiguration();
        }

        currentTimestamp = analyticsUtil.getTimestamp(toDate);

        String formattedToDate = toDate.concat(ChoreoAnalyticsConstants.TIMEZONE_FORMAT);
        String fromDate = simpleDateFormat.format(
                new Date(lastPublishInfo.getLastPublishTime()));
        String formattedFromDate = fromDate.concat(ChoreoAnalyticsConstants.TIMEZONE_FORMAT);

        //get usageData with all the dependency variables
        String queryApiEndpoint = config.getMonetizationConfigurationDto().getInsightAPIEndpoint();
        String onPremKey = config.getMonetizationConfigurationDto().getAnalyticsAccessToken();
        if (StringUtils.isEmpty(queryApiEndpoint) || StringUtils.isEmpty(onPremKey)) {
            // Since on prem key is required for both query APIs, it has been made mandatory
            throw new AnalyticsException(
                    "Endpoint or analytics access token for the the analytics query api is not configured");
        }

        String accessToken;
        if (MonetizationDataHolder.getInstance().getMonetizationAccessTokenGenerator() != null) {
            accessToken = MonetizationDataHolder.getInstance().getMonetizationAccessTokenGenerator().getAccessToken();
            if (StringUtils.isEmpty(accessToken)) {
                throw new AnalyticsException(
                        "Cannot retrieve access token from the provided token url");
            }
            useNewQueryAPI = true;
        } else {
            accessToken = onPremKey;
            useNewQueryAPI = false;
        }

        JSONObject timeFilter = new JSONObject();
        timeFilter.put(ChoreoAnalyticsConstants.FROM, formattedFromDate);
        timeFilter.put(ChoreoAnalyticsConstants.TO, formattedToDate);

        List<JSONArray> tenantsAndApis = analyticsUtil.getMonetizedAPIIdsAndTenantDomains();

        if (tenantsAndApis.size() == 2) { //it should always be 2
            if (tenantsAndApis.get(1).size() > 0) {
                JSONObject successAPIUsageByAppFilter = new JSONObject();
                successAPIUsageByAppFilter.put(ChoreoAnalyticsConstants.API_ID_COL, tenantsAndApis.get(1));
                successAPIUsageByAppFilter.put(TENANT_DOMAIN_COL, tenantsAndApis.get(0));
                JSONObject variables = new JSONObject();
                variables.put(ChoreoAnalyticsConstants.TIME_FILTER, timeFilter);
                variables.put(ChoreoAnalyticsConstants.API_USAGE_BY_APP_FILTER, successAPIUsageByAppFilter);
                if (useNewQueryAPI) {
                    variables.put(ChoreoAnalyticsConstants.ON_PREM_KEY, onPremKey);//puts which query to use
                }
                GraphQLClient graphQLClient =
                        Feign.builder().client(new OkHttpClient()).encoder(new GsonEncoder()).decoder(new GsonDecoder())
                                .logger(new Slf4jLogger())
                                .requestInterceptor(new QueryAPIAccessTokenInterceptor(accessToken))
                                .target(GraphQLClient.class, queryApiEndpoint);
                GraphqlQueryModel queryModel = new GraphqlQueryModel();
                queryModel.setQuery(getGraphQLQueryBasedOnTheOperationMode(useNewQueryAPI));
                queryModel.setVariables(variables);
                graphQLResponseClient usageResponse = graphQLClient.getSuccessAPIsUsageByApplications(queryModel); //gets the data here

                LinkedTreeMap<String, ArrayList<LinkedTreeMap<String, String>>> data = usageResponse.getData();

                //processing takes place here
                ArrayList<LinkedTreeMap<String, String>> usageData = new ArrayList<>();
                if (data != null) {
                    usageData = data.get((useNewQueryAPI) ?
                            ChoreoAnalyticsConstants.GET_USAGE_BY_APPLICATION_WITH_ON_PREM_KEY
                            : ChoreoAnalyticsConstants.GET_USAGE_BY_APPLICATION);
                }

                if (usageData.isEmpty()) {
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug("No API Usage retrived for the given period of time");
                        }
                        //last publish time will be updated as successfully since there was no usage retrieved.
                        lastPublishInfo.setLastPublishTime(currentTimestamp);
                        lastPublishInfo.setState(AnalyticsConstants.COMPLETED);
                        lastPublishInfo.setStatus(AnalyticsConstants.SUCCESSFULL);
                        apiAdmin.updateMonetizationUsagePublishInfo(lastPublishInfo);
                    } catch (APIManagementException ex) {
                        String msg = "Failed to update last published time ";
                        throw new AnalyticsException(msg, ex);
                    }
                    return null;
                }

                List<MonetizationDTO> monetizationInfo = new ArrayList<>();

                for (Map.Entry<String, ArrayList<LinkedTreeMap<String, String>>> entry : data.entrySet()){
                    //String key = entry.getKey();
                    //since key is never used it's been commented out
                    ArrayList<LinkedTreeMap<String, String>> apiUsageDataCollection = entry.getValue();
                    for (LinkedTreeMap<String, String> apiUsageData : apiUsageDataCollection) {
                        apiUuid = apiUsageData.get(API_UUID);
                        tenantDomain = apiUsageData.get(ChoreoAnalyticsConstants.TENANT_DOMAIN);
                        applicationName = apiUsageData.get(ChoreoAnalyticsConstants.APPLICATION_NAME);
                        applicationOwner = apiUsageData.get(ChoreoAnalyticsConstants.APPLICATION_OWNER);
                        requestCount = Long.parseLong(apiUsageData.get(ChoreoAnalyticsConstants.COUNT));

                        MonetizationDTO usageInfo = new MonetizationDTO(currentTimestamp, apiUuid, tenantDomain,
                                applicationName, applicationOwner, null, requestCount);
                        monetizationInfo.add(usageInfo);
                    }
                }

                return monetizationInfo;
            }
        }
        return null;
    }

    public String getGraphQLQueryBasedOnTheOperationMode(boolean useNewQueryAPI) {

        if (useNewQueryAPI) {
            return "query($onPremKey: String!, $timeFilter: TimeFilter!, " +
                    "$successAPIUsageByAppFilter: SuccessAPIUsageByAppFilter!) " +
                    "{getSuccessAPIsUsageByApplicationsWithOnPremKey(onPremKey:$onPremKey, timeFilter: $timeFilter, " +
                    "successAPIUsageByAppFilter: $successAPIUsageByAppFilter) { apiId apiName apiVersion " +
                    "apiCreatorTenantDomain applicationId applicationName applicationOwner count}}";
        } else {
            return "query($timeFilter: TimeFilter!, " +
                    "$successAPIUsageByAppFilter: SuccessAPIUsageByAppFilter!) " +
                    "{getSuccessAPIsUsageByApplications(timeFilter: $timeFilter, " +
                    "successAPIUsageByAppFilter: $successAPIUsageByAppFilter) { apiId apiName apiVersion " +
                    "apiCreatorTenantDomain applicationId applicationName applicationOwner count}}";
        }
    }
}