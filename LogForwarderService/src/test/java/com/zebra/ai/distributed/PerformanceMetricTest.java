package com.zebra.ai.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** The demo application's "PerformanceMetrics" log line, and the message the dashboard server gets for it. */
public class PerformanceMetricTest {

    private static PerformanceMetric parsed(String line) {
        PerformanceMetric metric = PerformanceMetric.parse(line);
        assertNotNull("not understood: " + line, metric);
        return metric;
    }

    /* ================= The lines as the demo application logs them ================= */

    @Test
    public void theBareMessage() {
        PerformanceMetric metric = parsed("mode=LOCAL fps=3.39 peers=2/10 lastMs=328");
        assertEquals("LOCAL", metric.mode);
        assertEquals(3.39, metric.fps, 0.0);
        assertEquals(Long.valueOf(328), metric.lastMs);
    }

    @Test
    public void everyLineOfTheExamples() {
        String[][] examples = {
                {"mode=LOCAL fps=3.39 peers=2/10 lastMs=328", "LOCAL", "3.39", "328"},
                {"mode=LOCAL fps=3.28 peers=2/10 lastMs=376", "LOCAL", "3.28", "376"},
                {"mode=LOCAL fps=3.20 peers=2/10 lastMs=203", "LOCAL", "3.2", "203"},
                {"mode=THROUGHPUT fps=0.00 peers=2/10 lastMs=730", "THROUGHPUT", "0.0", "730"},
                {"mode=THROUGHPUT fps=0.90 peers=2/10 lastMs=2212", "THROUGHPUT", "0.9", "2212"},
                {"mode=THROUGHPUT fps=1.14 peers=2/10 lastMs=1207", "THROUGHPUT", "1.14", "1207"},
                {"mode=LOCAL fps=15.36 peers=4/10 lastMs=57", "LOCAL", "15.36", "57"},
        };
        for (String[] example : examples) {
            PerformanceMetric metric = parsed(example[0]);
            assertEquals(example[1], metric.mode);
            assertEquals(Double.parseDouble(example[2]), metric.fps, 0.0);
            assertEquals(Long.valueOf(example[3]), metric.lastMs);
        }
    }

    @Test
    public void theLineBeforeTheFirstResult_hasNoLastMs_andIsStillAMetric() {
        // the application's second format string: "mode=%s fps=%.2f peers=%d/%d"
        PerformanceMetric metric = parsed("mode=LOCAL fps=0.00 peers=4/10");
        assertEquals("LOCAL", metric.mode);
        assertEquals(0.0, metric.fps, 0.0);
        assertNull(metric.lastMs);
        assertEquals("{\"type\":\"perf\",\"source\":\"PerformanceMetrics\",\"mode\":\"LOCAL\",\"fps\":0.00,\"lastMs\":null}",
                metric.toJson());
    }

    /* ================= Whatever way logcat prints it ================= */

    @Test
    public void inLogcatsBriefFormat() {
        PerformanceMetric metric = parsed("I/PerformanceMetrics( 4983): mode=LOCAL fps=1.38 peers=4/10 lastMs=1093");
        assertEquals("LOCAL", metric.mode);
        assertEquals(1.38, metric.fps, 0.0);
        assertEquals(Long.valueOf(1093), metric.lastMs);
    }

    @Test
    public void inLogcatsThreadtimeFormat() {
        PerformanceMetric metric = parsed("10-07 21:21:31.212  4983  4983 I PerformanceMetrics: mode=THROUGHPUT fps=2.39 peers=4/10 lastMs=432");
        assertEquals("THROUGHPUT", metric.mode);
        assertEquals(2.39, metric.fps, 0.0);
        assertEquals(Long.valueOf(432), metric.lastMs);
    }

    @Test
    public void inLogcatsEpochFormat() {
        PerformanceMetric metric = parsed("         1791388294.320  7432  7432 I PerformanceMetrics: mode=LOCAL fps=1.00 peers=0/0 lastMs=1");
        assertEquals("LOCAL", metric.mode);
        assertEquals(Long.valueOf(1), metric.lastMs);
    }

    @Test
    public void inAndroidStudiosFormat_asPastedFromItsLogcatWindow() {
        PerformanceMetric metric = parsed("2026-10-07 21:21:31.212  4983-4983  PerformanceMetrics      com...apture.distributedaisuitedemo  I  mode=LOCAL fps=1.38 peers=4/10 lastMs=1093");
        assertEquals("LOCAL", metric.mode);
        assertEquals(1.38, metric.fps, 0.0);
        assertEquals(Long.valueOf(1093), metric.lastMs);
    }

    @Test
    public void withATrailingBlank_orCarriageReturn() {
        assertEquals("LOCAL", parsed("mode=LOCAL fps=1.00 peers=1/1 lastMs=5 ").mode);
        assertEquals(Long.valueOf(5), parsed("mode=LOCAL fps=1.00 peers=1/1 lastMs=5\r").lastMs);
    }

    @Test
    public void theWordsInAnyOrder_withOthersBetween() {
        PerformanceMetric metric = parsed("lastMs=5 extra=yes fps=2.50 peers=1/1 mode=LOCAL");
        assertEquals("LOCAL", metric.mode);
        assertEquals(2.5, metric.fps, 0.0);
        assertEquals(Long.valueOf(5), metric.lastMs);
    }

    @Test
    public void aCommaAsTheDecimalMark() {
        // a device set to a language that writes it that way
        PerformanceMetric metric = parsed("mode=LOCAL fps=1,38 peers=4/10 lastMs=1093");
        assertEquals(1.38, metric.fps, 0.0);
    }

    @Test
    public void aFractionOfAMillisecond_isRounded() {
        assertEquals(Long.valueOf(1093), parsed("mode=LOCAL fps=1.38 peers=4/10 lastMs=1092.6").lastMs);
        assertEquals(Long.valueOf(1092), parsed("mode=LOCAL fps=1.38 peers=4/10 lastMs=1092.4").lastMs);
    }

    @Test
    public void otherModeNames() {
        assertEquals("LOCAL_ONLY", parsed("mode=LOCAL_ONLY fps=1.00 lastMs=1").mode);
        assertEquals("round-robin.2", parsed("mode=round-robin.2 fps=1.00 lastMs=1").mode);
    }

    /* ================= What is not a performance line ================= */

    @Test
    public void banners_andOtherOutput_arenotMetrics() {
        assertNull(PerformanceMetric.parse("--------- beginning of main"));
        assertNull(PerformanceMetric.parse("logcat: Unexpected EOF!"));
        assertNull(PerformanceMetric.parse("I/OtherTag( 12): started, peers=2/10"));
        assertNull(PerformanceMetric.parse(""));
        assertNull(PerformanceMetric.parse(null));
    }

    @Test
    public void modeAndFpsAreNeeded() {
        assertNull(PerformanceMetric.parse("mode=LOCAL peers=2/10 lastMs=328"));
        assertNull(PerformanceMetric.parse("fps=3.39 peers=2/10 lastMs=328"));
        assertNull(PerformanceMetric.parse("mode= fps=3.39 lastMs=328"));
    }

    @Test
    public void aKeyHasToBeAWordOfItsOwn() {
        assertNull(PerformanceMetric.parse("xmode=LOCAL fps=1.00 lastMs=1"));
        assertNull(PerformanceMetric.parse("mode=LOCAL xfps=1.00 lastMs=1"));
        assertNull("lastMs inside another word is not lastMs", parsed("mode=LOCAL fps=1.00 xlastMs=9").lastMs);
    }

    @Test
    public void aNumberThatIsNotANumber_isNotAMetric() {
        assertNull(PerformanceMetric.parse("mode=LOCAL fps=NaN peers=1/1 lastMs=1"));
        assertNull(PerformanceMetric.parse("mode=LOCAL fps=abc peers=1/1 lastMs=1"));
        assertNull(PerformanceMetric.parse("mode=LOCAL fps=1.0.0 peers=1/1 lastMs=1"));
    }

    @Test
    public void aBadLastMs_leavesItOut_butKeepsTheRest() {
        PerformanceMetric metric = parsed("mode=LOCAL fps=1.00 peers=1/1 lastMs=--");
        assertNull(metric.lastMs);
        assertEquals("LOCAL", metric.mode);
    }

    @Test
    public void aModeThatCouldBreakTheMessage_isNotAMode() {
        assertNull(PerformanceMetric.parse("mode=\"LOCAL fps=1.00 lastMs=1"));
        assertNull(PerformanceMetric.parse("mode=LOCAL\"} fps=1.00 lastMs=1"));
    }

    @Test
    public void aVeryLongLine_isNotLookedAt() {
        StringBuilder text = new StringBuilder("mode=LOCAL fps=1.00 peers=1/1 lastMs=5 ");
        for (int i = 0; i < 200; i++) {
            text.append("padding padding padding ");
        }
        assertNull(PerformanceMetric.parse(text.toString()));
    }

    /* ================= The message ================= */

    @Test
    public void theMessage_hasTheModeTheFrameRateAndTheLastMs() {
        assertEquals("{\"type\":\"perf\",\"source\":\"PerformanceMetrics\",\"mode\":\"LOCAL\",\"fps\":1.38,\"lastMs\":1093}",
                parsed("mode=LOCAL fps=1.38 lastMs=1093").toJson());
        assertEquals("{\"type\":\"perf\",\"source\":\"PerformanceMetrics\",\"mode\":\"THROUGHPUT\",\"fps\":0.00,\"lastMs\":730}",
                parsed("mode=THROUGHPUT fps=0.00 lastMs=730").toJson());
    }

    @Test
    public void theMessage_hasTheCapabilityWhenTheLineHasOne_andNeverThePeers() {
        assertEquals("{\"type\":\"perf\",\"source\":\"PerformanceMetrics\",\"mode\":\"LOCAL\",\"fps\":39.22,\"lastMs\":45,"
                        + "\"capability\":\"ocr.recognize\"}",
                parsed("mode=LOCAL capability=ocr.recognize fps=39.22 peers=0/3 lastMs=45").toJson());
        // the line as Android Studio's logcat window shows it
        PerformanceMetric metric = parsed("2026-10-12 17:40:12.185 30957-30957 PerformanceMetrics      com...apture.distributedaisuitedemo  I  "
                + "mode=LOCAL capability=ocr.recognize fps=39.22 peers=0/3 lastMs=45");
        assertEquals("ocr.recognize", metric.capability);
        assertEquals(Long.valueOf(45), metric.lastMs);
        assertFalse(metric.toJson().contains("peers"));
    }

    @Test
    public void aLineWithoutACapability_hasNone_andTheMessageLeavesItOut() {
        PerformanceMetric metric = parsed("mode=LOCAL fps=3.39 peers=2/10 lastMs=328");
        assertNull(metric.capability);
        assertEquals("{\"type\":\"perf\",\"source\":\"PerformanceMetrics\",\"mode\":\"LOCAL\",\"fps\":3.39,\"lastMs\":328}", metric.toJson());
    }

    @Test
    public void theCapabilityIsSentAsTheApplicationWroteIt() {
        // case and spelling are the dashboard's business ("Ocr.Recognise" is "ocr.recognize")
        assertEquals("Ocr.Recognise", parsed("mode=LOCAL capability=Ocr.Recognise fps=3.39 peers=2/10 lastMs=328").capability);
        assertEquals("product.embed_lookup", parsed("capability=product.embed_lookup mode=THROUGHPUT fps=1.00 lastMs=9").capability);
        assertEquals("product.frame", parsed("mode=LOCAL fps=1.00 lastMs=9 capability=product.frame").capability);
    }

    @Test
    public void aCapabilityThatCouldBreakTheMessage_orIsNotAWord_isLeftOut() {
        assertNull(parsed("mode=LOCAL capability=\"ocr fps=1.00 lastMs=1").capability);
        assertNull(parsed("mode=LOCAL capability=ocr\"} fps=1.00 lastMs=1").capability);
        assertNull(parsed("mode=LOCAL xcapability=ocr.recognize fps=1.00 lastMs=1").capability);
        assertNull(parsed("mode=LOCAL capability= fps=1.00 lastMs=1").capability);
        assertNull("longer than any capability", parsed("mode=LOCAL capability=" + new String(new char[49]).replace('\0', 'a') + " fps=1.00 lastMs=1").capability);
    }

    @Test
    public void theFrameRateIsWrittenWithTwoDecimals() {
        assertTrue(parsed("mode=LOCAL fps=15.4 lastMs=1").toJson().contains("\"fps\":15.40,"));
        assertTrue(parsed("mode=LOCAL fps=3 lastMs=1").toJson().contains("\"fps\":3.00,"));
        assertTrue(parsed("mode=LOCAL fps=1.386 lastMs=1").toJson().contains("\"fps\":1.39,"));
    }

    @Test
    public void thePeersAreNotSent() {
        assertFalse(parsed("mode=LOCAL fps=1.38 peers=4/10 lastMs=1093").toJson().contains("peers"));
        assertFalse(parsed("mode=LOCAL capability=ocr.recognize fps=1.38 peers=4/10 lastMs=1093").toJson().contains("peers"));
    }

    @Test
    public void theTypeAndTheTagAreTheOnesTheServerIsTold() {
        assertEquals("perf", PerformanceMetric.MESSAGE_TYPE);
        assertEquals("PerformanceMetrics", PerformanceMetric.LOG_TAG);
    }
}
