package org.wso2.am.analytics.retriever.elk;

import org.wso2.am.analytics.retriever.AnalyticsConstants;

/**
 * This class is to define constants related to ELK based analytics
 */
public class ELKAnalyticsConstants extends AnalyticsConstants {
    /**
     * ELK based analytics related constants
     **/

    public static final String DEFAULT_ELK_ANALYTICS_INDEX = "apim_event_response";
    public static final String ELK_API_ID_COL = "apiId.keyword";
    public static final String ELK_TENANT_DOMAIN = "apiCreatorTenantDomain.keyword";
    public static final String ELK_APPLICATION_ID_COLUMN = "applicationId.keyword";
    public static final String REQUEST_TIMESTAMP_COLUMN = "requestTimestamp";
    public static final String APPLICATION_ID_COLUMN = "applicationId";

}
