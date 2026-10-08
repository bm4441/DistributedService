package com.zebra.ai.distributed;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One line of the demo application's performance log -- logcat tag "PerformanceMetrics", written once per
 * processed frame -- and the message the log forwarder sends the dashboard server for it:
 *
 *   log line:  mode=LOCAL fps=1.38 peers=4/10 lastMs=1093
 *   message:   {"type":"perf","source":"PerformanceMetrics","mode":"LOCAL","fps":1.38,"lastMs":1093}
 *
 * Only the mode, the frame rate and the last processing time are sent ("peers" is not). Before the first result
 * the application leaves lastMs out ("mode=LOCAL fps=0.00 peers=4/10"): the line is still sent, with lastMs null.
 *
 * The line is found by its "key=value" words wherever they are in the text, so it does not matter which logcat
 * format the line comes in -- the bare text, "I/PerformanceMetrics( 4983): mode=...", the threadtime style with
 * its date and thread ids, or Android Studio's -- and the words may come in any order, with other words between.
 *
 * Plain Java, no Android: it runs in plain unit tests.
 */
final class PerformanceMetric {

    /** The logcat tag the application logs under; the log forwarder asks logcat for this tag only. */
    static final String LOG_TAG = "PerformanceMetrics";
    static final String MESSAGE_TYPE = "perf";
    static final String SOURCE_NAME = LOG_TAG;

    // A real line is under 80 characters; anything much longer is not one, and is not worth looking through.
    private static final int MAX_LINE_LENGTH = 1024;

    // A word is "key=" after the start of the text or a blank, so that "lastMs=" is not found in "xlastMs=".
    // Numbers are plain decimals; a comma is accepted as the decimal mark (a device set to such a language).
    private static final Pattern MODE = Pattern.compile("(?:^|\\s)mode=([A-Za-z0-9_.\\-]{1,32})(?=\\s|$)");
    private static final Pattern FPS = Pattern.compile("(?:^|\\s)fps=(-?\\d+(?:[.,]\\d+)?)(?=\\s|$)");
    private static final Pattern LAST_MS = Pattern.compile("(?:^|\\s)lastMs=(-?\\d+(?:[.,]\\d+)?)(?=\\s|$)");

    /** LOCAL, THROUGHPUT, ... exactly as the application wrote it. */
    final String mode;
    final double fps;
    /** The last processing time in milliseconds; null if the line had none yet. */
    final Long lastMs;

    private PerformanceMetric(String mode, double fps, Long lastMs) {
        this.mode = mode;
        this.fps = fps;
        this.lastMs = lastMs;
    }

    /** The metric in a logcat line, or null if the line is not a performance line (a banner, other output, damaged). */
    static PerformanceMetric parse(String line) {
        if (line == null || line.length() > MAX_LINE_LENGTH) {
            return null;
        }
        Matcher mode = MODE.matcher(line);
        Matcher fps = FPS.matcher(line);
        if (!mode.find() || !fps.find()) {
            return null;                                   // mode and fps are what a performance line is
        }
        Long lastMs = null;
        Matcher last = LAST_MS.matcher(line);
        if (last.find()) {
            lastMs = Math.round(number(last.group(1)));
        }
        return new PerformanceMetric(mode.group(1), number(fps.group(1)), lastMs);
    }

    private static double number(String text) {
        return Double.parseDouble(text.replace(',', '.'));
    }

    /** The message for the dashboard server: the frame rate with two decimals, as the application writes it. */
    String toJson() {
        JsonText json = new JsonText().beginObject();
        json.key("type").str(MESSAGE_TYPE);
        json.key("source").str(SOURCE_NAME);
        json.key("mode").str(mode);
        json.key("fps").num(fps, 2);
        json.key("lastMs");
        if (lastMs != null) {
            json.num(lastMs);
        } else {
            json.nul();
        }
        return json.endObject().toString();
    }

    @Override
    public String toString() {
        return "mode=" + mode + " fps=" + fps + " lastMs=" + (lastMs != null ? lastMs : "-");
    }
}
