package org.wso2.am.analytics.retriever.elk;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.transport.ElasticsearchTransport;
import org.elasticsearch.client.RestClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.json.simple.JSONArray;
import org.wso2.am.analytics.retriever.AnalyticsConstants;
import org.wso2.am.analytics.retriever.AnalyticsUtil;
import org.wso2.carbon.apimgt.api.APIAdmin;
import org.wso2.carbon.apimgt.api.APIManagementException;
import org.wso2.carbon.apimgt.api.MonetizationException;
import org.wso2.carbon.apimgt.api.model.*;
import org.wso2.carbon.apimgt.common.analytics.exceptions.AnalyticsException;
import org.wso2.carbon.apimgt.impl.APIAdminImpl;
import org.wso2.carbon.apimgt.impl.APIManagerConfiguration;
import org.wso2.carbon.apimgt.impl.dao.ApiMgtDAO;
import org.wso2.carbon.apimgt.impl.internal.ServiceReferenceHolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class ELKAnalyticsforMonetizationImpl implements AnalyticsforMonetization {

    private static final Log log = LogFactory.getLog(ELKAnalyticsforMonetizationImpl.class);
    private static APIManagerConfiguration config = null;
    AnalyticsUtil analyticsUtil = new AnalyticsUtil();
    private ApiMgtDAO apiMgtDAO = ApiMgtDAO.getInstance();

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
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(ELKAnalyticsConstants.TIME_FORMAT);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone(ELKAnalyticsConstants.TIME_ZONE));
        String toDate = simpleDateFormat.format(dateobj);

        if (config == null){
            // Retrieve the access token from api manager configurations.
            config = ServiceReferenceHolder.getInstance().getAPIManagerConfigurationService().
                    getAPIManagerConfiguration();
        }

        currentTimestamp = analyticsUtil.getTimestamp(toDate);

        String formattedToDate = toDate.concat(ELKAnalyticsConstants.TIMEZONE_FORMAT);
        String fromDate = simpleDateFormat.format(
                new Date(lastPublishInfo.getLastPublishTime()));
        String formattedFromDate = fromDate.concat(ELKAnalyticsConstants.TIMEZONE_FORMAT);

        String username = config.getMonetizationConfigurationDto().getAnalyticsUserName();
        byte[] password = config.getMonetizationConfigurationDto().getAnalyticsPassword();
        String hostname = config.getMonetizationConfigurationDto().getAnalyticsHost();
        String analyticsIndex = config.getMonetizationConfigurationDto().getAnalyticsIndexName();
        if (analyticsIndex == null) {
            analyticsIndex = ELKAnalyticsConstants.DEFAULT_ELK_ANALYTICS_INDEX;
        }
        int port = config.getMonetizationConfigurationDto().getAnalyticsPort();
        List<JSONArray> tenantDomainsAndAPIs = analyticsUtil.getMonetizedAPIIdsAndTenantDomains();
        JSONArray tenants = tenantDomainsAndAPIs.get(0);
        JSONArray monetizedAPIs = tenantDomainsAndAPIs.get(1);
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,
                new String(password, StandardCharsets.UTF_8)));

        SearchResponse<Object> searchResponse;

        if (tenantDomainsAndAPIs.size() == 2) {
            List<FieldValue> tenantList = new ArrayList<>();
            List<FieldValue> monetizedAPIsList = new ArrayList<>();
            for (Object tenant : tenants) {
                tenantList.add(new FieldValue.Builder().stringValue((String) tenant).build());
            }
            for (Object api : monetizedAPIs) {
                monetizedAPIsList.add(new FieldValue.Builder().stringValue((String) api).build());
            }
            try (RestClient restClient = RestClient.builder(new HttpHost(hostname, port))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider)).build()) {
                ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                ElasticsearchClient elasticsearchClient = new ElasticsearchClient(transport);

                Query query = BoolQuery.of(b -> b
                        .must(QueryBuilders.range(r -> r.field(ELKAnalyticsConstants.REQUEST_TIMESTAMP_COLUMN)
                                .from(formattedFromDate).to(formattedToDate)))
                        .must(QueryBuilders.terms(m -> m.field(ELKAnalyticsConstants.ELK_API_ID_COL)
                                .terms(t -> t.value(monetizedAPIsList))))
                        .must(QueryBuilders.terms(l -> l.field(ELKAnalyticsConstants.ELK_TENANT_DOMAIN)
                                .terms(t -> t.value(tenantList)))))._toQuery();

                SearchRequest searchRequest = new SearchRequest.Builder()
                        .index(analyticsIndex)
                        .query(query)
                        .aggregations(ELKAnalyticsConstants.API_UUID, a -> a
                                .terms(TermsAggregation.of(t -> t
                                        .field(ELKAnalyticsConstants.ELK_API_ID_COL)))
                                .aggregations(ELKAnalyticsConstants.TENANT_DOMAIN_COL, b -> b
                                        .terms(TermsAggregation.of(t -> t
                                                .field(ELKAnalyticsConstants.ELK_TENANT_DOMAIN)))
                                        .aggregations(ELKAnalyticsConstants.APPLICATION_ID_COLUMN, c -> c
                                                .terms(TermsAggregation.of(t -> t
                                                        .field(ELKAnalyticsConstants.ELK_APPLICATION_ID_COLUMN))))))
                        .source(s -> s.fetch(false)).size(0).build();

                searchResponse =  elasticsearchClient.search(searchRequest, Object.class);
            } catch (IOException e) {
                throw new AnalyticsException("Error occurred while executing data retrieval from Elasticsearch", e);
            }
        } else {
            try (RestClient restClient = RestClient.builder(new HttpHost(hostname, port))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider)).build()) {
                ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
                ElasticsearchClient elasticsearchClient = new ElasticsearchClient(transport);
                Query query = RangeQuery.of(r -> r.field(ELKAnalyticsConstants.REQUEST_TIMESTAMP_COLUMN)
                                .from(formattedFromDate)
                                .to(formattedToDate))
                        ._toQuery();

                SearchRequest searchRequest = new SearchRequest.Builder()
                        .index(analyticsIndex)
                        .query(query)
                        .aggregations(ELKAnalyticsConstants.API_UUID, a -> a.terms(
                                        TermsAggregation.of(t -> t.field(ELKAnalyticsConstants.ELK_API_ID_COL)))
                                .aggregations(ELKAnalyticsConstants.APPLICATION_ID_COLUMN, b -> b.terms(
                                        TermsAggregation.of(t -> t.field(ELKAnalyticsConstants.ELK_APPLICATION_ID_COLUMN)))))
                        .source(s -> s.fetch(false)).size(0).build();
                searchResponse = elasticsearchClient.search(searchRequest, Object.class);
            } catch (IOException e) {
                throw new AnalyticsException("Error occurred while executing data retrieval from Elasticsearch", e);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Collecting data from elasticsearch within the time range from" + fromDate + " to "
                    + toDate);
        }

        List<StringTermsBucket> apiIdBuckets = searchResponse.aggregations()
                .get(ELKAnalyticsConstants.API_UUID).sterms().buckets().array();

        if (apiIdBuckets.isEmpty()) {
            try {
                log.debug("No API Usage retrieved for the given period of time");
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

        for (StringTermsBucket apiIdBucketObj : apiIdBuckets){
            MonetizationDTO usageInfo = null;
            apiUuid = apiIdBucketObj.key().stringValue();
            List<StringTermsBucket> tenantBasedBuckets = apiIdBucketObj.aggregations().get(AnalyticsConstants.TENANT_DOMAIN_COL)
                    .sterms().buckets().array();
            for (StringTermsBucket tenantBasedBucket : tenantBasedBuckets){
                tenantDomain = tenantBasedBucket.key().stringValue();
                List<StringTermsBucket> appIdBuckets = tenantBasedBucket.aggregations()
                        .get(ELKAnalyticsConstants.APPLICATION_ID_COLUMN).sterms().buckets().array();
                for (StringTermsBucket appIdBucketObj : appIdBuckets){
                    String applicationUuid = appIdBucketObj.key().stringValue();
                    requestCount = appIdBucketObj.docCount();
                    try {
                        Application app = apiMgtDAO.getApplicationByUUID(applicationUuid);
                        applicationName = app.getName();
                        applicationOwner = app.getOwner();
                        usageInfo = new MonetizationDTO(currentTimestamp, apiUuid, tenantDomain, applicationName,
                                applicationOwner, null, requestCount);

                    } catch (APIManagementException e){
                        String errorMessage = "Unable to retrieve App instance for " + applicationUuid;
                        throw new AnalyticsException(errorMessage, e);
                    }
                }
            }
            monetizationInfo.add(usageInfo);
        }
        return monetizationInfo;
    }
}
