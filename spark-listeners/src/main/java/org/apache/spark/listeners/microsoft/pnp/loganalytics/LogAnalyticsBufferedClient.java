package org.apache.spark.listeners.microsoft.pnp.loganalytics;

import java.util.LinkedHashMap;
import java.util.concurrent.Future;

public class LogAnalyticsBufferedClient {
    private final LinkedHashMap<String, Buffer> buffers = new LinkedHashMap();

    private final LogAnalyticsClient client;
    private final String messageType;

    public LogAnalyticsBufferedClient(LogAnalyticsClient client, String messageType) {
        // We'll make this configurable later.
        this.client = client;
        this.messageType = messageType;
    }

    public Future<Void> sendMessageAsync(String message, String timeGeneratedField) {
        // Get buffer for bucketing, in this case, time-generated field
        // since we limit the client to a specific message type.
        // This is because different event types can have differing time fields (i.e. Spark)
        Buffer buffer = this.getBuffer(timeGeneratedField);
        return buffer.sendMessageAsync(message);
    }

    private synchronized Buffer getBuffer(String timeGeneratedField) {
        Buffer buffer = this.buffers.get(timeGeneratedField);
        if (null == buffer) {
            buffer = new Buffer(this.client, timeGeneratedField);
            this.buffers.put(timeGeneratedField, buffer);
        }

        return buffer;
    }
}
