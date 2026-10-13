package com.zebra.ai.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ScanCaptureTest {

    private static final String QR = "ws://10.117.131.123:5000/logs";

    /** Feeds the text one character per stepMillis from t0; returns the time of the last character. */
    private static long type(ScanCapture c, String s, long t0, long stepMillis) {
        long t = t0;
        for (int i = 0; i < s.length(); i++) {
            c.onChar(s.charAt(i), t);
            if (i < s.length() - 1) {
                t += stepMillis;
            }
        }
        return t;
    }

    @Test
    public void scannerBurstIsAScan() {
        ScanCapture c = new ScanCapture();
        long last = type(c, QR, 1000, 3);
        assertTrue(c.looksLikeScan());
        assertNull("not complete yet", c.finish(last + 100));
        assertEquals(QR, c.finish(last + ScanCapture.SETTLE_MILLIS));
        assertTrue(c.isEmpty());
    }

    @Test
    public void personTypingIsNotAScan() {
        ScanCapture c = new ScanCapture();
        long last = type(c, "ws://10.1.2.3:5000/logs", 1000, 100);   // 100 ms per key: a fast typist
        assertFalse(c.looksLikeScan());
        assertNull(c.finish(last + ScanCapture.SETTLE_MILLIS));
        assertTrue(c.isEmpty());
    }

    @Test
    public void slowTypingBreaksIntoRunsOfOne() {
        ScanCapture c = new ScanCapture();
        assertTrue(c.onChar('a', 0));
        assertTrue("a pause longer than MAX_GAP starts a new run", c.onChar('b', ScanCapture.MAX_GAP_MILLIS + 1));
        assertFalse(c.onChar('c', ScanCapture.MAX_GAP_MILLIS + 2));
    }

    @Test
    public void tooShortIsNotAScan() {
        ScanCapture c = new ScanCapture();
        long last = type(c, "abc", 0, 2);
        assertFalse(c.looksLikeScan());
        assertNull(c.finish(last + ScanCapture.SETTLE_MILLIS));
    }

    @Test
    public void wholeStringInOneEventIsAScan() {
        ScanCapture c = new ScanCapture();
        assertTrue(c.onChunk(QR, 500));
        assertTrue(c.looksLikeScan());
        assertEquals(QR, c.finish(500 + ScanCapture.SETTLE_MILLIS));
    }

    @Test
    public void emptyChunkIsIgnored() {
        ScanCapture c = new ScanCapture();
        assertFalse(c.onChunk("", 0));
        assertFalse(c.onChunk(null, 0));
        assertTrue(c.isEmpty());
    }

    @Test
    public void enterRightAfterAScanIsTakenButAPersonsEnterIsNot() {
        ScanCapture c = new ScanCapture();
        long last = type(c, QR, 0, 3);
        assertTrue(c.takesEnter(last + 5));
        assertFalse("long after the last character", c.takesEnter(last + ScanCapture.MAX_GAP_MILLIS + 50));

        ScanCapture typed = new ScanCapture();
        long lastTyped = type(typed, "ws://1.2.3.4", 0, 150);
        assertFalse(typed.takesEnter(lastTyped + 10));
        assertFalse(new ScanCapture().takesEnter(0));
    }

    @Test
    public void twoScansInARowAreSeparate() {
        ScanCapture c = new ScanCapture();
        long last = type(c, QR, 0, 3);
        assertEquals(QR, c.finish(last + ScanCapture.SETTLE_MILLIS));
        String second = "ws://192.168.0.9:5000/logs";
        assertTrue(c.onChar(second.charAt(0), last + 2000));
        long last2 = type(c, second.substring(1), last + 2003, 3);
        assertEquals(second, c.finish(last2 + ScanCapture.SETTLE_MILLIS));
    }

    @Test
    public void scanStartingRightAfterTypingKeepsOnlyTheScan() {
        ScanCapture c = new ScanCapture();
        c.onChar('x', 0);                                            // a person typed a character earlier
        long start = 2000;
        assertTrue(c.onChar('w', start));                            // the scan begins a new run
        long last = type(c, QR.substring(1), start + 3, 3);
        assertEquals(QR, c.finish(last + ScanCapture.SETTLE_MILLIS));
    }

    @Test
    public void resetForgetsEverything() {
        ScanCapture c = new ScanCapture();
        type(c, QR, 0, 3);
        c.reset();
        assertTrue(c.isEmpty());
        assertNull(c.finish(10_000));
    }

    @Test
    public void finishWhileStillRunningKeepsTheRun() {
        ScanCapture c = new ScanCapture();
        long last = type(c, QR, 0, 3);
        assertNull(c.finish(last + 10));
        assertFalse(c.isEmpty());
    }
}
