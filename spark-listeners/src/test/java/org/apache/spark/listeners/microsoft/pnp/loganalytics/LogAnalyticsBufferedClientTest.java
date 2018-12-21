package org.apache.spark.listeners.microsoft.pnp.loganalytics;

import static org.mockito.Mockito.*;

import org.junit.*;

import java.io.IOException;

public class LogAnalyticsBufferedClientTest {

    @Test
    public void batchShouldTimeoutAndCallClient() throws IOException {
        System.out.println("batchShouldTimeoutAndCallClient");
        //MyClass tester = new MyClass(); // MyClass is tested
        LogAnalyticsClient logAnalyticsClient = mock(LogAnalyticsClient.class);
        LogAnalyticsBufferedClient client = new LogAnalyticsBufferedClient(
                logAnalyticsClient,
                "TestMessageType",
                LogAnalyticsBufferedClient.DEFAULT_MAX_MESSAGE_SIZE_IN_BYTES,
                2000
        );

        client.sendMessage("testing", null);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException ie) {
            System.out.println("interrupted");
        }

        verify(logAnalyticsClient, times(1))
                .send(anyString(), anyString());
        // assert statements
//        assertEquals(0, tester.multiply(10, 0), "10 x 0 must be 0");
//        assertEquals(0, tester.multiply(0, 10), "0 x 10 must be 0");
//        assertEquals(0, tester.multiply(0, 0), "0 x 0 must be 0");
    }

    @Test
    public void batchSizeShouldExceedByteLimitAndCallSendTwice() throws IOException {
        System.out.println("batchSizeShouldExceedByteLimitAndCallSendTwice");
        //MyClass tester = new MyClass(); // MyClass is tested
        LogAnalyticsClient logAnalyticsClient = mock(LogAnalyticsClient.class);
        LogAnalyticsBufferedClient client = new LogAnalyticsBufferedClient(
                logAnalyticsClient,
                "TestMessageType",
                50, 3000
        );

        client.sendMessage("I am a big, long, string of characters.", null);
        client.sendMessage("I am another big, long, string of characters.", null);
        try {
            Thread.sleep(7000);
        } catch (InterruptedException ie) {
            System.out.println("interrupted");
        }

        verify(logAnalyticsClient, times(2))
                .send(anyString(), anyString());
        // assert statements
//        assertEquals(0, tester.multiply(10, 0), "10 x 0 must be 0");
//        assertEquals(0, tester.multiply(0, 10), "0 x 10 must be 0");
//        assertEquals(0, tester.multiply(0, 0), "0 x 0 must be 0");
    }
}
