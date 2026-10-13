package com.zebra.ai.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class ScannedAddressTest {

    @Test
    public void dashboardQrCodeIsAccepted() {
        assertEquals("ws://10.117.131.123:5000/logs", ScannedAddress.parse("ws://10.117.131.123:5000/logs"));
    }

    @Test
    public void secureSchemeIsAccepted() {
        assertEquals("wss://logs.example.com:443/logs", ScannedAddress.parse("wss://logs.example.com:443/logs"));
    }

    @Test
    public void lineBreakAndControlCharactersFromTheScannerAreDropped() {
        assertEquals("ws://10.117.131.123:5000/logs", ScannedAddress.parse("ws://10.117.131.123:5000/logs\r\n"));
        assertEquals("ws://10.117.131.123:5000/logs", ScannedAddress.parse("\tws://10.117.131.123:5000/logs\n"));
    }

    @Test
    public void upperCaseSchemeIsAccepted() {
        assertEquals("ws://10.0.0.5:5000/logs", ScannedAddress.parse("WS://10.0.0.5:5000/logs"));
    }

    @Test
    public void otherCodesAreRejected() {
        assertNull(ScannedAddress.parse("https://example.com/product/42"));
        assertNull(ScannedAddress.parse("http://10.117.131.123:5000/logs"));
        assertNull(ScannedAddress.parse("10.117.131.123:5000"));
        assertNull(ScannedAddress.parse("5901234123457"));
        assertNull(ScannedAddress.parse("hello world"));
        assertNull(ScannedAddress.parse("ws://10.0.0.5:5000/logs and more"));
    }

    @Test
    public void emptyAndNullAreRejected() {
        assertNull(ScannedAddress.parse(null));
        assertNull(ScannedAddress.parse(""));
        assertNull(ScannedAddress.parse("\r\n"));
    }
}
