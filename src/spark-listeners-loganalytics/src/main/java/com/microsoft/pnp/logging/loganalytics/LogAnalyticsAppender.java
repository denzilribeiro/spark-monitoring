package com.microsoft.pnp.logging.loganalytics;

import com.microsoft.pnp.LogAnalyticsEnvironment;
import com.microsoft.pnp.client.loganalytics.LogAnalyticsClient;
import com.microsoft.pnp.client.loganalytics.LogAnalyticsSendBufferClient;
import com.microsoft.pnp.logging.JSONLayout;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.commons.lang3.StringUtils;

import static com.microsoft.pnp.logging.JSONLayout.TIMESTAMP_FIELD_NAME;

public class LogAnalyticsAppender extends AppenderSkeleton {
    private static final Filter ORG_APACHE_HTTP_FILTER = new Filter() {
        @Override
        public int decide(LoggingEvent loggingEvent) {
            if (loggingEvent.getLoggerName().startsWith("org.apache.http")) {
                return Filter.DENY;
            }
            return Filter.ACCEPT;
        }
    };


    // We will default to environment so the properties file can override
    private String workspaceId = LogAnalyticsEnvironment.getWorkspaceId();
    private String secret = LogAnalyticsEnvironment.getWorkspaceKey();
    private String clusterId = LogAnalyticsEnvironment.getClusterId();
    private String isSparkDriver = LogAnalyticsEnvironment.isDriverNode();
    private String logType = LogAnalyticsEnvironment.getLog4jTableName();
    private LogAnalyticsSendBufferClient client;

    public LogAnalyticsAppender() {
        this.addFilter(ORG_APACHE_HTTP_FILTER);
    }

    @Override
    public void activateOptions() {
        this.client = new LogAnalyticsSendBufferClient(
                new LogAnalyticsClient(this.workspaceId, this.secret),
                this.logType
        );
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        if (this.layout == null) {
            this.setLayout(new JSONLayout());
        }
        // Add 2 custom properties so that cluster can be identified and whether is driver or not
        if (!StringUtils.isEmpty(clusterId))
            loggingEvent.setProperty("clusterId",clusterId);

        if (!StringUtils.isEmpty(isSparkDriver))
            loggingEvent.setProperty("isDriverNode",isSparkDriver);

        String json = this.getLayout().format(loggingEvent);
        this.client.sendMessage(json, TIMESTAMP_FIELD_NAME);
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public void setLayout(Layout layout) {
        // This will allow us to configure the layout from properties to add custom JSON stuff.
        if (!(layout instanceof JSONLayout)) {
            throw new UnsupportedOperationException("layout must be an instance of JSONLayout");
        }

        super.setLayout(layout);
    }

    @Override
    public void clearFilters() {
        super.clearFilters();
        // We need to make sure to add the filter back so we don't get stuck in a loop
        this.addFilter(ORG_APACHE_HTTP_FILTER);
    }

    public String getWorkspaceId() {return this.workspaceId;}
    public String getClusterId() {
        return this.clusterId;
    }

    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getSecret() {
        return this.secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getLogType() {
        return this.logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }
}
