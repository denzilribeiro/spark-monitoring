package org.apache.spark.listeners.microsoft.pnp.loganalytics;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

class Buffer {
    private final String timeGeneratedField;

    private final SendBuffer sendBuffer;
    /**
     * This executor that will be shared among all buffers. We may not need this, since we
     * shouldn't have hundreds of different time generated fields, but we can't executors spinning
     * up hundreds of threads.
     *
     * The DaemonThreadFactory creates daemon threads, which means they won't block
     * the JVM from exiting if only they are still around.
     */
    static ExecutorService executor = Executors.newCachedThreadPool(new DaemonThreadFactory());

    public Buffer(LogAnalyticsClient client, String timeGeneratedField) {
        this.timeGeneratedField = timeGeneratedField;
        this.sendBuffer = new SendBuffer(client, this.executor);
    }

    public Future<Void> sendMessageAsync(String message) {
        ClientFuture future = this.sendBuffer.sendMessage(message);
        future.setBuffer(this);
        return future;
    }

    /**
     * We need daemon threads in our executor so that we don't keep the process running if our
     * executor threads are the only ones left in the process.
     *
     * This may need to change, as there could be a crash-like situation where we want diagnostics
     * sent if there is any way possible.  Something to consider.
     */
    private static class DaemonThreadFactory implements ThreadFactory {
        static AtomicInteger threadCount = new AtomicInteger(0);

        public Thread newThread(Runnable r) {
            int threadNumber = threadCount.addAndGet(1);
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("BufferWorkerThread-" + threadNumber);
            return thread;
        }

    }
}
