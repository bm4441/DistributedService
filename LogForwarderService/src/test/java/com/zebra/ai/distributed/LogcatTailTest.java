package com.zebra.ai.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

/**
 * The follower of logcat: its command, how long it waits before running it again, and -- with another JVM standing in
 * for logcat -- what it does when the command prints, ends, fails, hangs or cannot be started at all.
 */
public class LogcatTailTest {

    private static final LogcatTail.Timing QUICK = new LogcatTail.Timing(50, 400, 60_000);

    /** The stand-in for logcat: another JVM that prints something and ends, or does not end. */
    public static final class FakeLogcat {
        public static void main(String[] args) throws Exception {
            switch (args[0]) {
                case "lines":
                    System.out.println("--------- beginning of main");
                    System.out.println("I/PerformanceMetrics( 4983): mode=LOCAL fps=1.38 peers=4/10 lastMs=1093");
                    System.out.println("I/PerformanceMetrics( 4983): mode=THROUGHPUT fps=2.26 peers=4/10 lastMs=507");
                    System.out.flush();
                    break;
                case "error":
                    System.err.println("logcat: Unexpected EOF!");
                    System.err.flush();
                    System.exit(1);
                    break;
                case "stderr":
                    System.err.println("on standard error");
                    System.err.flush();
                    break;
                case "hang":
                    // prints one line, then never ends; if given a file it counts up in it, so a test can tell it is alive
                    System.out.println("I/PerformanceMetrics( 4983): mode=LOCAL fps=1.00 peers=1/1 lastMs=5");
                    System.out.flush();
                    for (long beat = 0; ; beat++) {
                        if (args.length > 1) {
                            Files.write(Paths.get(args[1]), Long.toString(beat).getBytes(StandardCharsets.UTF_8));
                        }
                        Thread.sleep(50);
                    }
                default:
                    System.exit(2);
            }
        }
    }

    private static List<String> fake(String mode, String... more) {
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator
                + (System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java");
        List<String> command = new ArrayList<>(Arrays.asList(java, "-cp", classesDir(), FakeLogcat.class.getName(), mode));
        command.addAll(Arrays.asList(more));
        return command;
    }

    /** The count in a heartbeat file (see FakeLogcat "hang"); -1 if it could not be read just now. */
    private static long heartbeat(File file) {
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                String text = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8).trim();
                if (!text.isEmpty()) {
                    return Long.parseLong(text);
                }
            } catch (IOException | NumberFormatException e) {
                // being written at this moment: try again
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return -1;
    }

    private static String classesDir() {
        try {
            return new File(LogcatTailTest.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    static class Recorder implements LogcatTail.Listener {
        final List<String> lines = new CopyOnWriteArrayList<>();
        final List<String> notices = new CopyOnWriteArrayList<>();

        @Override
        public void onLine(String line) {
            lines.add(line);
        }

        @Override
        public void onNotice(String text) {
            notices.add(text);
        }

        int count(String fragment) {
            int n = 0;
            for (String notice : notices) {
                if (notice.contains(fragment)) {
                    n++;
                }
            }
            return n;
        }

        String first(String fragment) {
            for (String notice : notices) {
                if (notice.contains(fragment)) {
                    return notice;
                }
            }
            return null;
        }
    }

    private static void waitUntil(BooleanSupplier condition, long millis) throws InterruptedException {
        long end = System.currentTimeMillis() + millis;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < end) {
            Thread.sleep(20);
        }
    }

    /* ================= The command ================= */

    @Test
    public void theCommand_asksForTheTagFromAMomentOn_inBriefLines() {
        assertEquals(Arrays.asList("logcat", "-v", "brief", "-T", "1791388294.320", "PerformanceMetrics:V", "*:S"),
                LogcatTail.command("PerformanceMetrics", 1791388294320L));
    }

    @Test
    public void theMomentIsSecondsAndThreeDigitsOfMilliseconds() {
        assertEquals("1700000000.005", LogcatTail.command("X", 1700000000005L).get(4));
        assertEquals("1700000000.000", LogcatTail.command("X", 1700000000000L).get(4));
        assertEquals("1700000000.999", LogcatTail.command("X", 1700000000999L).get(4));
    }

    /* ================= The wait before running it again ================= */

    @Test
    public void theDefaultTiming() {
        assertEquals(5_000L, LogcatTail.Timing.DEFAULT.initialRetryMillis);
        assertEquals(300_000L, LogcatTail.Timing.DEFAULT.maxRetryMillis);
        assertEquals(60_000L, LogcatTail.Timing.DEFAULT.stableRunMillis);
    }

    @Test
    public void aCommandThatKeepsEndingSoon_isRunAgainLessAndLessOften_upToTheMaximum() {
        LogcatTail.Timing t = LogcatTail.Timing.DEFAULT;
        assertEquals(5_000L, LogcatTail.nextRetryMillis(t, 0, 100));
        assertEquals(15_000L, LogcatTail.nextRetryMillis(t, 5_000, 100));
        assertEquals(45_000L, LogcatTail.nextRetryMillis(t, 15_000, 100));
        assertEquals(135_000L, LogcatTail.nextRetryMillis(t, 45_000, 100));
        assertEquals("405 s is over the maximum", 300_000L, LogcatTail.nextRetryMillis(t, 135_000, 100));
        assertEquals(300_000L, LogcatTail.nextRetryMillis(t, 300_000, 100));
    }

    @Test
    public void aRunThatLasted_startsTheSequenceOver() {
        LogcatTail.Timing t = LogcatTail.Timing.DEFAULT;
        assertEquals(5_000L, LogcatTail.nextRetryMillis(t, 300_000, 60_000));
        assertEquals(5_000L, LogcatTail.nextRetryMillis(t, 135_000, 3_600_000));
        assertEquals("one millisecond short of it still counts as ending soon", 15_000L,
                LogcatTail.nextRetryMillis(t, 5_000, 59_999));
    }

    /* ================= With a command that really runs ================= */

    @Test
    public void theLinesOfTheCommand_reachTheListener_inOrder_andItsEndIsNoticed() throws Exception {
        Recorder recorder = new Recorder();
        LogcatTail tail = new LogcatTail("test", now -> fake("lines"), recorder, QUICK);
        tail.start();
        try {
            waitUntil(() -> recorder.lines.size() >= 3 && recorder.count("Ended after") >= 1, 20_000);
            assertEquals(Arrays.asList(
                    "--------- beginning of main",
                    "I/PerformanceMetrics( 4983): mode=LOCAL fps=1.38 peers=4/10 lastMs=1093",
                    "I/PerformanceMetrics( 4983): mode=THROUGHPUT fps=2.26 peers=4/10 lastMs=507"),
                    new ArrayList<>(recorder.lines.subList(0, 3)));
            assertTrue(recorder.notices.toString(), recorder.count("Started: ") >= 1);
            assertTrue(recorder.first("Ended after"), recorder.first("Ended after").contains("(exit 0)"));
        } finally {
            tail.stop();
        }
    }

    @Test
    public void whenTheCommandEnds_itIsRunAgain() throws Exception {
        Recorder recorder = new Recorder();
        LogcatTail tail = new LogcatTail("test", now -> fake("lines"), recorder, QUICK);
        tail.start();
        try {
            waitUntil(() -> recorder.count("Started: ") >= 3, 30_000);
            assertTrue("started " + recorder.count("Started: ") + " time(s)", recorder.count("Started: ") >= 3);
            assertTrue("each run printed its lines", recorder.lines.size() >= 6);
        } finally {
            tail.stop();
        }
    }

    @Test
    public void eachRunIsGivenTheTimeItStartsAt() throws Exception {
        final List<Long> times = new CopyOnWriteArrayList<>();
        Recorder recorder = new Recorder();
        long before = System.currentTimeMillis();
        LogcatTail tail = new LogcatTail("test", now -> {
            times.add(now);
            return fake("lines");
        }, recorder, QUICK);
        tail.start();
        try {
            waitUntil(() -> times.size() >= 2, 20_000);
            long after = System.currentTimeMillis();
            assertTrue(times.size() >= 2);
            assertTrue(times.get(0) >= before && times.get(0) <= after);
            assertTrue("a later run is given a later time", times.get(1) > times.get(0));
        } finally {
            tail.stop();
        }
    }

    @Test
    public void aFailedRun_saysWhatTheCommandPrinted_andItsExitCode() throws Exception {
        Recorder recorder = new Recorder();
        LogcatTail tail = new LogcatTail("test", now -> fake("error"), recorder, QUICK);
        tail.start();
        try {
            waitUntil(() -> recorder.count("Ended after") >= 1, 20_000);
            String ended = recorder.first("Ended after");
            assertTrue(ended, ended != null && ended.contains("(exit 1)"));
            assertTrue(ended, ended.contains("logcat: Unexpected EOF!"));
            assertTrue("the line is also given to the listener", recorder.lines.contains("logcat: Unexpected EOF!"));
        } finally {
            tail.stop();
        }
    }

    @Test
    public void standardError_isReadToo() throws Exception {
        Recorder recorder = new Recorder();
        LogcatTail tail = new LogcatTail("test", now -> fake("stderr"), recorder, QUICK);
        tail.start();
        try {
            waitUntil(() -> recorder.lines.contains("on standard error"), 20_000);
            assertTrue(recorder.lines.contains("on standard error"));
        } finally {
            tail.stop();
        }
    }

    @Test
    public void aCommandThatCannotBeStarted_isReported_andTriedAgain() throws Exception {
        Recorder recorder = new Recorder();
        LogcatTail tail = new LogcatTail("test", now -> Arrays.asList("this-command-does-not-exist-xyz"), recorder, QUICK);
        tail.start();
        try {
            waitUntil(() -> recorder.count("Could not run or read it") >= 2, 20_000);
            assertTrue(recorder.notices.toString(), recorder.count("Could not run or read it") >= 2);
            assertEquals("never started", 0, recorder.count("Started: "));
        } finally {
            tail.stop();
        }
    }

    @Test
    public void aListenerThatFails_doesNotEndTheReading() throws Exception {
        Recorder recorder = new Recorder() {
            @Override
            public void onLine(String line) {
                super.onLine(line);
                if (lines.size() == 1) {
                    throw new IllegalStateException("boom");
                }
            }
        };
        LogcatTail tail = new LogcatTail("test", now -> fake("lines"), recorder, QUICK);
        tail.start();
        try {
            waitUntil(() -> recorder.lines.size() >= 3, 20_000);
            assertTrue("the lines after the failing one still arrive", recorder.lines.size() >= 3);
            assertTrue(recorder.notices.toString(), recorder.count("The listener failed") >= 1);
        } finally {
            tail.stop();
        }
    }

    /* ================= Stopping ================= */

    @Test
    public void stop_endsACommandThatDoesNotEnd_promptly_andNothingIsRunAfterwards() throws Exception {
        final File beats = File.createTempFile("logcat-tail-test", ".heartbeat");
        beats.deleteOnExit();
        Recorder recorder = new Recorder();
        LogcatTail tail = new LogcatTail("test", now -> fake("hang", beats.getPath()), recorder, QUICK);
        tail.start();
        waitUntil(() -> recorder.lines.size() >= 1 && heartbeat(beats) >= 3, 20_000);
        assertTrue("the command is running and printing", recorder.lines.size() >= 1);
        long beatsBefore = heartbeat(beats);
        assertTrue("and alive: its heartbeat counts up (" + beatsBefore + ")", beatsBefore >= 3);

        long startedNanos = System.nanoTime();
        tail.stop();
        long stopMillis = (System.nanoTime() - startedNanos) / 1_000_000;
        assertTrue("stop took " + stopMillis + " ms", stopMillis < 4_000);

        Thread.sleep(300);                                   // whatever it had in flight is written by now
        long beatsAfterStop = heartbeat(beats);
        Thread.sleep(500);
        assertEquals("the command is gone: its heartbeat has stopped", beatsAfterStop, heartbeat(beats));

        int starts = recorder.count("Started: ");
        Thread.sleep(500);
        assertEquals("nothing is started after the stop", starts, recorder.count("Started: "));
        assertEquals("a stop is not an end to report", 0, recorder.count("Ended after"));
    }

    @Test
    public void stop_whileItWaitsToRunTheCommandAgain_isImmediate() throws Exception {
        Recorder recorder = new Recorder();
        LogcatTail tail = new LogcatTail("test", now -> fake("error"), recorder, new LogcatTail.Timing(60_000, 60_000, 60_000));
        tail.start();
        waitUntil(() -> recorder.count("Ended after") >= 1, 20_000);
        assertEquals("it is now waiting a minute", 1, recorder.count("running it again in 60 s"));
        long startedNanos = System.nanoTime();
        tail.stop();
        long stopMillis = (System.nanoTime() - startedNanos) / 1_000_000;
        assertTrue("stop took " + stopMillis + " ms", stopMillis < 1_500);
    }

    @Test
    public void aStoppedTail_doesNotStartAgain_andStoppingTwiceIsHarmless() throws Exception {
        Recorder recorder = new Recorder();
        LogcatTail tail = new LogcatTail("test", now -> fake("lines"), recorder, QUICK);
        tail.stop();
        tail.stop();
        tail.start();
        Thread.sleep(500);
        assertEquals(0, recorder.count("Started: "));
        assertTrue(recorder.lines.isEmpty());
    }

    @Test
    public void startingTwice_runsOneCommand() throws Exception {
        Recorder recorder = new Recorder();
        LogcatTail tail = new LogcatTail("test", now -> fake("hang"), recorder, QUICK);
        tail.start();
        tail.start();
        try {
            waitUntil(() -> recorder.lines.size() >= 1, 20_000);
            assertEquals("the command printed its line", 1, recorder.lines.size());
            Thread.sleep(300);                                // time enough for a second command to start, if there were one
            assertEquals(1, recorder.count("Started: "));
        } finally {
            tail.stop();
        }
    }
}
