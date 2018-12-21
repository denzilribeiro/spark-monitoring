package org.apache.spark.listeners.microsoft.pnp.loganalytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendBuffer {

    private static final Logger logger = LoggerFactory.getLogger(SendBuffer.class);

    /**
     * This executor that will be shared among all buffers. We may not need this, since we
     * shouldn't have hundreds of different time generated fields, but we can't executors spinning
     * up hundreds of threads.
     *
     * The DaemonThreadFactory creates daemon threads, which means they won't block
     * the JVM from exiting if only they are still around.
     */
    //static ExecutorService executor = Executors.newCachedThreadPool(new SendBuffer.DaemonThreadFactory());
    static ExecutorService executor = Executors.newCachedThreadPool();

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
            logger.debug(String.format("Creating new executor thread %s", thread.getName()));
            return thread;
        }

    }

    // Interface to support event notifications with a parameter.
    private interface Listener<T> {
        void invoke(T o);
    };

    /** Config settings for this buffer */
    //private final QueueBufferConfig config;

    /** Url of our queue */
    //private final String qUrl;
    // We'll set this to 25MB, just in case.  LogAnalytics has a limit of 30 MB
    //private final int maxBatchSizeBytes = 1024 * 1024 * 25;
    private int maxBatchSizeBytes;

    // Set it to 10 seconds for now
    //private final int maxBatchOpenMs = 10000;
    private int maxBatchOpenMs;
    /**
     * The client to use for this buffer's operations.
     */
    private final LogAnalyticsClient client;

//    /**
//     * The executor service for the batching tasks.
//     */
//    private final Executor executor;

    /**
     * Object used to serialize sendMessage calls.
     */
    private final Object sendMessageLock = new Object();

    /**
     * Current batching task for sendMessage. Using a size 1 array to allow "passing by reference".
     * Synchronized by {@code sendMessageLock}.
     */
    //private final SendMessageBatchTask[] openSendMessageBatchTask = new SendMessageBatchTask[1];
    //private final SendRequestTask[] openSendMessageBatchTask = new SendRequestTask[1];
    private SendRequestTask sendRequestTask = null;

    /**
     * Permits controlling the number of in flight SendMessage batches.
     */
    private final Semaphore inflightBatches;

    // Make configurable
    private final int maxInflightBatches = 1;
    private final String timeGeneratedField;
    private final String logType;

    SendBuffer(LogAnalyticsClient client,
               String logType,
               String timeGenerateField,
               int maxMessageSizeInBytes,
               int batchTimeInMilliseconds
               ) {
        this.client = client;
        this.logType = logType;
        this.timeGeneratedField = timeGenerateField;
        this.maxBatchSizeBytes = maxMessageSizeInBytes;
        this.maxBatchOpenMs = batchTimeInMilliseconds;
        // must allow at least one outbound batch.
        //maxBatch = maxBatch > 0 ? maxBatch : 1;
        //this.inflightBatches = new Semaphore(maxBatch);
        this.inflightBatches = new Semaphore(this.maxInflightBatches);
    }

    public void sendMessage(String message) {
        try {
            synchronized (this.sendMessageLock) {
                if (this.sendRequestTask == null
                        || (!this.sendRequestTask.addRequest(message))) {
                    logger.debug("Creating new task");
                    // We need a new task because one of the following is true:
                    // 1.  We don't have one yet (i.e. first message!)
                    // 2.  The task is full
                    // 3.  The task's timeout elapsed
                    SendRequestTask obt = new SendRequestTask();
                    // Make sure we don't have too many in flight at once.
                    // This WILL block the calling code, but it's simpler than
                    // building a circular buffer, although we are sort of doing that. :)
                    // Not sure we need this yet!
                    logger.debug("Acquiring semaphore");
                    this.inflightBatches.acquire();
                    logger.debug("Acquired semaphore");
                    this.sendRequestTask = obt;

                    // Register a listener for the event signaling that the
                    // batch task has completed (successfully or not).
                    this.sendRequestTask.setOnCompleted(new Listener<SendRequestTask>() {
                        @Override
                        public void invoke(SendRequestTask task) {
                            logger.debug("Releasing semaphore");
                            inflightBatches.release();
                            logger.debug("Released semaphore");
                        }
                    });

//                    if (log.isTraceEnabled()) {
//                        log.trace("Queue " + qUrl + " created new batch for " + request.getClass().toString() + " "
//                                + inflightOperationBatches.availablePermits() + " free slots remain");
//                    }

                    // There is an edge case here.
                    // If the max bytes are too small for the first message, things go
                    // wonky, so let's bail
                    if (!this.sendRequestTask.addRequest(message)) {
                        throw new RuntimeException("Failed to schedule batch");
                    }
                    executor.execute(this.sendRequestTask);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            RuntimeException toThrow = new RuntimeException("Interrupted while waiting for lock.");
            toThrow.initCause(e);
            throw toThrow;
        }
    }

    /**
     * Flushes all outstanding outbound requests ({@code SendMessage}, {@code DeleteMessage},
     * {@code ChangeMessageVisibility}) in this buffer.
     * <p>
     * The call returns successfully when all outstanding outbound requests submitted before the
     * call are completed (i.e. processed by SQS).
     */
    public void flush() {

        try {
            synchronized (sendMessageLock) {
                inflightBatches.acquire(this.maxInflightBatches);
                inflightBatches.release(this.maxInflightBatches);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Task to send a batch of outbound requests to SQS.
     * <p>
     * The batch task is constructed open and accepts requests until full, or until
     * {@code maxBatchOpenMs} elapses. At that point, the batch closes and the collected requests
     * are assembled into a single batch request to SQS. Specialized for each type of outbound
     * request.
     * <p>
     * Instances of this class (and subclasses) are thread-safe.
     *
     */
    private class SendRequestTask implements Runnable {

        int batchSizeBytes = 0;
        protected final List<String> requests;
        private boolean closed;
        private volatile Listener<SendRequestTask> onCompleted;

        public SendRequestTask() {
            this.requests = new ArrayList<>();
        }

        //public void setOnCompleted(Listener<OutboundBatchTask<R, Result>> value) {
        public void setOnCompleted(Listener<SendRequestTask> value) {
            onCompleted = value;
        }

        /**
         * Adds a request to the batch if it is still open and has capacity.
         *
         * @return the future that can be used to get the results of the execution, or null if the
         *         addition failed.
         */
        public synchronized boolean addRequest(String request) {
            if (closed) {
                return false;
            }

            boolean wasAdded = addIfAllowed(request);
            // If we can't add the request (because we are full), close the batch
            if (!wasAdded) {
                logger.debug("Could not add.  Closing");
                closed = true;
                notify();
            }

            return wasAdded;
        }

        /**
         * Adds the request to the batch if capacity allows it. Called by {@code addRequest} with a
         * lock on {@code this} held.
         *
         * @param request
         * @return the future that will be signaled when the request is completed and can be used to
         *         retrieve the result. Can be null if the addition could not be done
         */
        private boolean addIfAllowed(String request) {

            if (isOkToAdd(request)) {
                logger.debug("Allowed to add");
                requests.add(request);
                onRequestAdded(request);
                return true;

            } else {
                return false;
            }
        }

        /**
         * Checks whether it's okay to add the request to this buffer. Called by
         * {@code addIfAllowed} with a lock on {@code this} held.
         *
         * @param request
         *            the request to add
         * @return true if the request is okay to add, false otherwise
         */
        protected boolean isOkToAdd(String request) {
            return ((request.getBytes().length + batchSizeBytes) <= maxBatchSizeBytes);
        }

        /**
         * A hook to be run when a request is successfully added to this buffer. Called by
         * {@code addIfAllowed} with a lock on {@code this} held.
         *
         * @param request
         *            the request that was added
         */
        protected void onRequestAdded(String request) {
            batchSizeBytes += request.getBytes().length;
        }

        /**
         * Processes the batch once closed. Is <em>NOT</em> called with a lock on {@code this}.
         * However, it's passed a local copy of both the {@code requests} and {@code futures} lists
         * made while holding the lock.
         */
        protected void process(List<String> requests) {
            if (requests.isEmpty()) {
                logger.debug("No requests to send");
                return;
            }

            // Build up Log Analytics "batch" and send.
            // How should we handle failures?  I think there is retry built into the HttpClient,
            // but what if that fails as well?  I suspect we should just log it and move on.

            // We are going to assume that the requests are properly formatted
            // JSON strings.  So for now, we are going to just wrap brackets around
            // them.
            StringBuffer sb = new StringBuffer("[");
            for (String request : requests) {
                sb.append(request).append(",");
            }
            sb.deleteCharAt(sb.lastIndexOf(",")).append("]");
            try {
                logger.debug(sb.toString());
                client.send(sb.toString(), logType, timeGeneratedField);
            } catch (IOException ioe) {
                logger.error(ioe.getMessage(), ioe);
            }
        }

        @Override
        public final void run() {
            try {

                long deadlineMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
                        + maxBatchOpenMs + 1;
                long t = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);

                List<String> requests;

                synchronized (this) {
                    while (!closed && (t < deadlineMs)) {
                        t = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);

                        // zero means "wait forever", can't have that.
                        long toWait = Math.max(1, deadlineMs - t);
                        wait(toWait);
                    }

                    closed = true;

                    requests = new ArrayList<>(this.requests);
                }

                logger.debug("Processing on thread " + Thread.currentThread().getName());
                process(requests);
                logger.debug("Processing complete");
            } catch (InterruptedException e) {
                logger.error("run was interrupted", e);
                failAll(e);
            } catch (RuntimeException e) {
                logger.error("RuntimeException", e);
                failAll(e);
                throw e;
            } catch (Error e) {
                Exception ex = new Exception("Error encountered", e);
                logger.error("Error encountered", ex);
                //failAll(new Exception("Error encountered", e));
                throw e;
            } finally {
                // make a copy of the listener since it (theoretically) can be
                // modified from the outside.
                Listener<SendRequestTask> listener = onCompleted;
                if (listener != null) {
                    logger.debug("Invoking completion callback.");
                    listener.invoke(this);
                }
            }
        }

        // There is nothing to "fail all" on...it's all one shot. :)
        private void failAll(Exception e) {
//            for (QueueBufferFuture<R, Result> f : futures) {
//            for (ClientFuture f : futures) {
//                f.setFailure(e);
//            }
        }
    }
}
