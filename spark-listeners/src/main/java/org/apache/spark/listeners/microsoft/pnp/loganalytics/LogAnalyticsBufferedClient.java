package org.apache.spark.listeners.microsoft.pnp.loganalytics;

import java.util.LinkedHashMap;
import java.util.concurrent.Future;

public class LogAnalyticsBufferedClient {
    private final LinkedHashMap<String, Buffer> buffers = new LinkedHashMap<>();

    private final LogAnalyticsClient client;
    private final String messageType;
    private final int maxMessageSizeInBytes;
    private final int batchTimeInMilliseconds;

    public static final int DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES = 1024 * 1024 * 25;
    public static final int DEFAULT_BATCH_TIME_IN_MILLISECONDS = 5000;

    public LogAnalyticsBufferedClient(LogAnalyticsClient client, String messageType) {
        this(
                client,
                messageType,
                DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES,
                DEFAULT_BATCH_TIME_IN_MILLISECONDS
        );
    }

    public LogAnalyticsBufferedClient(LogAnalyticsClient client,
                                      String messageType,
                                      int maxMessageSizeInBytes,
                                      int batchTimeInMilliseconds) {
        this.client = client;
        this.messageType = messageType;
        this.maxMessageSizeInBytes = maxMessageSizeInBytes;
        this.batchTimeInMilliseconds = batchTimeInMilliseconds;
    }

    //public Future<Void> sendMessageAsync(String message, String timeGeneratedField) {
    public void sendMessage(String message, String timeGeneratedField) {
        // Get buffer for bucketing, in this case, time-generated field
        // since we limit the client to a specific message type.
        // This is because different event types can have differing time fields (i.e. Spark)
        Buffer buffer = this.getBuffer(timeGeneratedField);
        //return buffer.sendMessageAsync(message);
        buffer.sendMessage(message);
    }

    private synchronized Buffer getBuffer(String timeGeneratedField) {
        System.out.println("Getting buffer for key: " + (timeGeneratedField == null ? "null" : timeGeneratedField));
        Buffer buffer = this.buffers.get(timeGeneratedField);
        if (null == buffer) {
            System.out.println("Buffer was null....creating");
            buffer = new Buffer(this.client, timeGeneratedField, this.maxMessageSizeInBytes,
                    this.batchTimeInMilliseconds);
            this.buffers.put(timeGeneratedField, buffer);
        }

        return buffer;
    }
}
