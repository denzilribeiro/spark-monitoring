package org.apache.spark.listeners.microsoft.pnp.loganalytics;

import java.awt.image.ImagingOpException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SendBuffer {
    //private static Log log = LogFactory.getLog(SendQueueBuffer.class);

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
     * The {@code AmazonSQS} client to use for this buffer's operations.
     */
    //private final AmazonSQS sqsClient;
    private final LogAnalyticsClient client;

    /**
     * The executor service for the batching tasks.
     */
    private final Executor executor;

    /**
     * Object used to serialize sendMessage calls.
     */
    private final Object sendMessageLock = new Object();

    /**
     * Current batching task for sendMessage. Using a size 1 array to allow "passing by reference".
     * Synchronized by {@code sendMessageLock}.
     */
    //private final SendMessageBatchTask[] openSendMessageBatchTask = new SendMessageBatchTask[1];
    private final SendRequestTask[] openSendMessageBatchTask = new SendRequestTask[1];

    /**
     * Permits controlling the number of in flight SendMessage batches.
     */
    private final Semaphore inflightSendMessageBatches;

    //SendQueueBuffer(AmazonSQS sqsClient, Executor executor, QueueBufferConfig paramConfig, String url) {
    SendBuffer(LogAnalyticsClient client, Executor executor,
               int maxMessageSizeInBytes, int batchTimeInMilliseconds) {
        //this.sqsClient = sqsClient;
        this.client = client;
        this.executor = executor;
        this.maxBatchSizeBytes = maxMessageSizeInBytes;
        this.maxBatchOpenMs = batchTimeInMilliseconds;
//        this.config = paramConfig;
//        qUrl = url;
//        int maxBatch = config.getMaxInflightOutboundBatches();

        // must allow at least one outbound batch.
        //maxBatch = maxBatch > 0 ? maxBatch : 1;
        //this.inflightSendMessageBatches = new Semaphore(maxBatch);
        this.inflightSendMessageBatches = new Semaphore(1);
    }

//    public QueueBufferConfig getConfig() {
//        return config;
//    }

    /**
     * @return never null
     */
//    public QueueBufferFuture<SendMessageRequest, SendMessageResult> sendMessage(SendMessageRequest request,
//                                                                                QueueBufferCallback<SendMessageRequest, SendMessageResult> callback) {
//        QueueBufferFuture<SendMessageRequest, SendMessageResult> result = submitOutboundRequest(sendMessageLock,
//                openSendMessageBatchTask, request, inflightSendMessageBatches, callback);
//        return result;
//    }
    //public ClientFuture sendMessage(String request) {
    public void sendMessage(String message) {
//        ClientFuture result = submitOutboundRequest(sendMessageLock,
//                openSendMessageBatchTask, request, inflightSendMessageBatches);
//        return result;
        submitOutboundRequest(sendMessageLock,
                this.openSendMessageBatchTask, message, inflightSendMessageBatches);
    }

//    /**
//     * @return new {@code OutboundBatchTask} of appropriate type, never null
//     */
//    @SuppressWarnings("unchecked")
//    private <R extends AmazonWebServiceRequest, Result> OutboundBatchTask<R, Result> newOutboundBatchTask(R request) {
//
//        if (request instanceof SendMessageRequest)
//            return (OutboundBatchTask<R, Result>) new SendMessageBatchTask();
//        else if (request instanceof DeleteMessageRequest)
//            return (OutboundBatchTask<R, Result>) new DeleteMessageBatchTask();
//        else if (request instanceof ChangeMessageVisibilityRequest)
//            return (OutboundBatchTask<R, Result>) new ChangeMessageVisibilityBatchTask();
//        else
//            // this should never happen
//            throw new IllegalArgumentException("Unsupported request type " + request.getClass().getName());
//    }

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
//                inflightSendMessageBatches.acquire(config.getMaxInflightOutboundBatches());
//                inflightSendMessageBatches.release(config.getMaxInflightOutboundBatches());
                inflightSendMessageBatches.acquire(1);
                inflightSendMessageBatches.release(1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Submits an outbound request for delivery to the queue associated with this buffer.
     * <p>
     *
     * @param operationLock
     *            the lock synchronizing calls for the call type ( {@code sendMessage},
     *            {@code deleteMessage}, {@code changeMessageVisibility} )
     * @param openOutboundBatchTask
     *            the open batch task for this call type
     * @param request
     *            the request to submit
     * @param inflightOperationBatches
     *            the permits controlling the batches for this type of request
     * @return never null
     */
    @SuppressWarnings("unchecked")
    private void submitOutboundRequest(Object operationLock,
                                       SendRequestTask[] openOutboundBatchTask,
                                       String request,
                                       final Semaphore inflightOperationBatches) {
        /*
         * Callers add requests to a single batch task (openOutboundBatchTask) until it is full or
         * maxBatchOpenMs elapses. The total number of batch task in flight is controlled by the
         * inflightOperationBatch semaphore capped at maxInflightOutboundBatches.
         */
        //ClientFuture theFuture = null;
        try {
            synchronized (operationLock) {
                if (openOutboundBatchTask[0] == null
                        || (!openOutboundBatchTask[0].addRequest(request))) {
                        //|| ((theFuture = openOutboundBatchTask[0].addRequest(request))) == null) {

                    System.out.println("Creating new task");
                    // We need a new task because one of the following is true:
                    // 1.  We don't have one yet (i.e. first message!)
                    // 2.  The task is full
                    // 3.  The task's timeout elapsed
                    SendRequestTask obt = new SendRequestTask();
                    // Make sure we don't have too many in flight at once.
                    // This WILL block the calling code, but it's simpler than
                    // building a circular buffer, although we are sort of doing that. :)
                    // Not sure we need this yet!
                    System.out.println("Acquiring semaphore");
                    inflightOperationBatches.acquire();
                    System.out.println("Acquired semaphore");
                    openOutboundBatchTask[0] = obt;

                    // Register a listener for the event signaling that the
                    // batch task has completed (successfully or not).
                    openOutboundBatchTask[0].setOnCompleted(new Listener<SendRequestTask>() {
                        @Override
                        public void invoke(SendRequestTask task) {
                            System.out.println("Releasing semaphore");
                            inflightOperationBatches.release();
                            System.out.println("Released semaphore");
                        }
                    });

//                    if (log.isTraceEnabled()) {
//                        log.trace("Queue " + qUrl + " created new batch for " + request.getClass().toString() + " "
//                                + inflightOperationBatches.availablePermits() + " free slots remain");
//                    }

                    //theFuture = openOutboundBatchTask[0].addRequest(request);
                    // There is an edge case here.
                    // If the max bytes are too small for the first message, things go
                    // wonky, so let's bail
                    if (!openOutboundBatchTask[0].addRequest(request)) {
                        throw new RuntimeException("Failed to schedule batch");
                    }
                    executor.execute(openOutboundBatchTask[0]);
//                    if (null == theFuture) {
//                        // this can happen only if the request itself is flawed,
//                        // so that it can't be added to any batch, even a brand
//                        // new one
//                        //throw new AmazonClientException("Failed to schedule request " + request + " for execution");
//                        throw new RuntimeException("Failed to schedule request");
//                    }
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            //AmazonClientException toThrow = new AmazonClientException("Interrupted while waiting for lock.");
            RuntimeException toThrow = new RuntimeException("Interrupted while waiting for lock.");
            toThrow.initCause(e);
            throw toThrow;
        }

        //return theFuture;
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
    //private abstract class OutboundBatchTask<R extends AmazonWebServiceRequest, Result> implements Runnable {
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
        //public synchronized QueueBufferFuture<R, Result> addRequest(R request, QueueBufferCallback<R, Result> callback) {
        public synchronized boolean addRequest(String request) {

            if (closed) {
                return false;
            }

//            QueueBufferFuture<R, Result> theFuture = addIfAllowed(request, callback);
            //ClientFuture theFuture = addIfAllowed(request);
            boolean wasAdded = addIfAllowed(request);
            // If we can't add the request (because we are full), close the batch
            //if ((null == theFuture) || isFull()) {
            if (!wasAdded) {
                System.out.println("Could not add.  Closing");
                closed = true;
                notify();
            }

            //return theFuture;
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
                System.out.println("Allowed to add");
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
         * Checks whether the buffer is now full. Called by {@code addIfAllowed} with a lock on
         * {@code this} held.
         *
         * @return whether the buffer is filled to capacity
         */
        protected boolean isFull() {
            return (batchSizeBytes >= maxBatchSizeBytes);
        }

        /**
         * Processes the batch once closed. Is <em>NOT</em> called with a lock on {@code this}.
         * However, it's passed a local copy of both the {@code requests} and {@code futures} lists
         * made while holding the lock.
         */
//        protected void process(List<R> requests, List<QueueBufferFuture<R, Result>> futures) {
        protected void process(List<String> requests) {
            if (requests.isEmpty()) {
                System.out.println("Empty!");
                return;
            }

            // Build up Log Analytics "batch" and send.
            // How should we handle failures?  I think there is retry built into the HttpClient,
            // but what if that fails as well?  I suspect we should just log it and move on.

            // Fake it for now because of tests
            try {
                client.send("blahblahblah", "need logtype!!!");
            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
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

                System.out.println("Processing on thread " + Thread.currentThread().getName());
                process(requests);

            } catch (InterruptedException e) {
                failAll(e);
//            } catch (AmazonClientException e) {
//                failAll(e);
            } catch (RuntimeException e) {
                failAll(e);
                throw e;
            } catch (Error e) {
                //failAll(new AmazonClientException("Error encountered", e));
                failAll(new Exception("Error encountered", e));
                throw e;
            } finally {
                // make a copy of the listener since it (theoretically) can be
                // modified from the outside.
                Listener<SendRequestTask> listener = onCompleted;
                if (listener != null) {
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
