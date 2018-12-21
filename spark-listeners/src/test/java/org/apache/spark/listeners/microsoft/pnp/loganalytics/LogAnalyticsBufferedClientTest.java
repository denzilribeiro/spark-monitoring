package org.apache.spark.listeners.microsoft.pnp.loganalytics;

import static org.mockito.Mockito.*;

import org.junit.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

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
                .send(anyString(), anyString(), (String)isNull());
        // assert statements
//        assertEquals(0, tester.multiply(10, 0), "10 x 0 must be 0");
//        assertEquals(0, tester.multiply(0, 10), "0 x 10 must be 0");
//        assertEquals(0, tester.multiply(0, 0), "0 x 0 must be 0");
    }

    @Test
    public void testReal() {
        System.out.println("testReal");
        LogAnalyticsClient logAnalyticsClient = new LogAnalyticsClient("",""
        );

        LogAnalyticsBufferedClient client = new LogAnalyticsBufferedClient(
                logAnalyticsClient, "TestLog3",
                1024 * 1024 * 2, 3000
        );

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(tz);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -30);
        for (int i = 0; i < 1000; i++) {
            String message = "{\"name\":\"message" + Integer.toString(i) + "\",\"value\":" +
                    Integer.toString(i) + ",\"myDate\":\"" + df.format(cal.getTime()) +"\"}";
            client.sendMessage(message, "myDate");
        }

        try {
            System.out.println("Sleeping 10 seconds");
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
            System.out.println("interrupted");
        }
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
                .send(anyString(), anyString(), (String)isNull());
        // assert statements
//        assertEquals(0, tester.multiply(10, 0), "10 x 0 must be 0");
//        assertEquals(0, tester.multiply(0, 10), "0 x 10 must be 0");
//        assertEquals(0, tester.multiply(0, 0), "0 x 0 must be 0");
    }
}
