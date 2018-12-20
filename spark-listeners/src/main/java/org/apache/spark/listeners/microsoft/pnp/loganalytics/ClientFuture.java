package org.apache.spark.listeners.microsoft.pnp.loganalytics;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ClientFuture implements Future<Void> {

    // Keep a reference so the buffer can't be garbage collected as long as at least one future
    // exists.
    private Buffer buffer = null;
    private Exception e = null;
    private boolean done = false;

    public ClientFuture() {

    }

    public synchronized void setBuffer(Buffer buffer) {
        this.buffer = buffer;
    }

    public synchronized void setSuccess() {
        if (this.done) {
            return;
        }

        this.done = true;
        this.notifyAll();
    }

    public synchronized void setFailure(Exception exception) {
        if (this.done) {
            return;
        }

        this.e = exception;
        this.done = true;
        this.notifyAll();
    }

    @Override
    public boolean cancel(boolean b) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public synchronized boolean isDone() {
        return this.done;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        while (true) {
            try {
                return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                // shouldn't really happen, since we're specifying a very-very
                // long wait. but if it does, just loop
                // and wait more.
            }
        }
    }

    @Override
    public Void get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        long waitStartMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        long timeoutMs = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
        long timeToWaitMs = timeoutMs;

        while (!this.done) {
            // if timeToWaitMs is zero, we don't call wait() at all, because wait(0) means
            // "wait forever", which is the opposite of what we want.
            if (timeToWaitMs <= 0) {
                throw new TimeoutException("Timed out waiting for results after " + timeout + " " + timeUnit);
            }

            wait(timeToWaitMs);

            // compute how long to wait in the next loop
            long nowMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
            timeToWaitMs = timeoutMs - (nowMs - waitStartMs);

        }

        // if we got here, we are done. Throw if there's anything to throw,
        // otherwise return the result
        if (this.e != null) {
            throw new ExecutionException(this.e);
        }

        // Since we are a Future<Void>.  Maybe we will return a result eventually?
        return null;
    }
}
