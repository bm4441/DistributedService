package com.zebra.ai.distributed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Follows logcat: runs the "logcat" command, hands every line it prints to a listener, and runs it again when it
 * ends -- on its own thread, so reading never holds anything up.
 *
 * logcat ends when the system stops it (or logd restarts, or the command cannot be started): an end is not an error
 * here, the command is run again after a delay that grows each time it ends soon after starting (5 s, 15 s, 45 s, ...
 * up to 5 minutes, see Timing), and starts over after a run that lasted (a command that ran for a minute was working).
 *
 * Android refusing to show an app other apps' log lines is not an end, though: since Android 12 a reader the system did
 * not approve keeps running and is given only the app's own lines (see LogAccess). The approval is a question to the
 * user, which Android only asks while the app is on screen -- so a run that matters has to be started at a moment the
 * caller chooses, which is what restartNow() is for.
 *
 * Each run only asks for what is logged from its start on (see command()): lines that were in logcat's buffer
 * before are old, and were not meant to be forwarded as if they were new.
 *
 * Plain Java, no Android: it runs in plain unit tests, with any command in place of logcat.
 */
final class LogcatTail {

    interface Listener {
        /** One line of the command's output (its standard error is mixed in), on the reading thread. Must not block. */
        void onLine(String line);

        /** What the reader is doing -- started, ended and why, about to start again -- for the forwarder's own log. */
        void onNotice(String text);

        /** A run of the command has started: its process is running. On the reading thread. */
        default void onRunStarted() {
        }

        /** A run of the command has ended by itself or on request -- not because the tail was stopped. On the reading thread. */
        default void onRunEnded() {
        }
    }

    /** Builds the command to run, given the time (milliseconds since 1970) it is about to be started at. */
    interface CommandFactory {
        List<String> create(long nowMillis);
    }

    /** How long to wait before running the command again, and how long a run has to last to count as having worked. */
    static final class Timing {
        static final Timing DEFAULT = new Timing(5_000L, 300_000L, 60_000L);

        final long initialRetryMillis;
        final long maxRetryMillis;
        final long stableRunMillis;

        Timing(long initialRetryMillis, long maxRetryMillis, long stableRunMillis) {
            this.initialRetryMillis = initialRetryMillis;
            this.maxRetryMillis = maxRetryMillis;
            this.stableRunMillis = stableRunMillis;
        }
    }

    private static final int LAST_LINES_KEPT = 3;
    private static final int LAST_LINE_MAX_CHARS = 200;
    private static final long REAP_MILLIS = 2_000L;
    private static final long STOP_JOIN_MILLIS = 2_500L;

    private final CommandFactory mCommands;
    private final Listener mListener;
    private final Timing mTiming;
    private final String mName;

    /** Guards the state below and the wait between runs. */
    private final Object mLock = new Object();
    private boolean mStopped;
    private boolean mRestartRequested;
    private Process mProcess;
    private Thread mThread;

    LogcatTail(String name, CommandFactory commands, Listener listener, Timing timing) {
        mName = name;
        mCommands = commands;
        mListener = listener;
        mTiming = timing;
    }

    /**
     * The command for logcat to print one tag as it is logged from the given moment on, as brief lines
     * ("I/Tag( 4983): text"): -T takes the time as seconds since 1970 with the milliseconds after a point, and
     * with a tag filter -- the tag at any priority, everything else silent -- it prints nothing that was logged before.
     */
    static List<String> command(String tag, long sinceMillis) {
        return command(tag, sinceMillis, new String[0]);
    }

    /** The same, with more tag filters ("logd:I") let through between the tag's own and the one that silences the rest. */
    static List<String> command(String tag, long sinceMillis, String... alsoFilters) {
        List<String> command = new ArrayList<>(Arrays.asList("logcat", "-v", "brief", "-T",
                String.format(Locale.US, "%d.%03d", sinceMillis / 1000, sinceMillis % 1000),
                tag + ":V"));
        command.addAll(Arrays.asList(alsoFilters));
        command.add("*:S");
        return command;
    }

    /**
     * The wait before the next run, given the wait before the one that just ended (0 for the first) and how long that
     * run lasted: the first wait is initialRetry, each wait after a short run is three times the one before, up to
     * maxRetry, and a run that lasted stableRun starts the sequence over.
     */
    static long nextRetryMillis(Timing timing, long previousRetryMillis, long ranMillis) {
        if (ranMillis >= timing.stableRunMillis || previousRetryMillis <= 0) {
            return timing.initialRetryMillis;
        }
        return Math.min(timing.maxRetryMillis, previousRetryMillis * 3);
    }

    /** Starts reading, on a thread of its own. A tail that was stopped cannot be started again: make another. */
    void start() {
        synchronized (mLock) {
            if (mThread != null || mStopped) {
                return;
            }
            mThread = new Thread(this::run, mName);
            mThread.setDaemon(true);
            mThread.start();
        }
    }

    /**
     * Ends the command now, if it is running, and runs it again at once -- without the wait a command that ended by
     * itself is given. For a run that has to start at a moment the caller picks (see the class comment). False if the
     * tail was never started, or is stopped.
     */
    boolean restartNow() {
        Process process;
        synchronized (mLock) {
            if (mThread == null || mStopped) {
                return false;
            }
            mRestartRequested = true;
            process = mProcess;
            mLock.notifyAll();                       // out of the wait between runs
        }
        if (process != null) {
            process.destroy();                       // its output ends, which ends the read
        }
        return true;
    }

    /** Stops the command and the reading, and waits a moment for the thread to end. Safe to call more than once. */
    void stop() {
        Process process;
        Thread thread;
        synchronized (mLock) {
            mStopped = true;
            process = mProcess;
            thread = mThread;
            mLock.notifyAll();                       // out of the wait between runs
        }
        if (process != null) {
            process.destroy();                       // its output ends, which ends the read
        }
        if (thread != null && thread != Thread.currentThread()) {
            thread.interrupt();
            try {
                thread.join(STOP_JOIN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean isStopped() {
        synchronized (mLock) {
            return mStopped;
        }
    }

    private void run() {
        long retryMillis = 0;
        while (!isStopped()) {
            long startedNanos = System.nanoTime();
            String lastOutput = runOnce();
            long ranMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
            if (isStopped()) {
                break;
            }
            runEnded();
            if (takeRestartRequest()) {
                retryMillis = 0;                     // asked for, so not a failure to count: the next end starts the waits over
                notice("Restarted on request after " + String.format(Locale.US, "%.1f", ranMillis / 1000.0) + " s");
                continue;
            }
            retryMillis = nextRetryMillis(mTiming, retryMillis, ranMillis);
            notice("Ended after " + String.format(Locale.US, "%.1f", ranMillis / 1000.0) + " s" + lastOutput
                    + "; running it again in " + (retryMillis / 1000) + " s");
            waitBeforeRetry(retryMillis);
        }
    }

    private boolean isRestartRequested() {
        synchronized (mLock) {
            return mRestartRequested;
        }
    }

    /** True, once, if restartNow() was called since the command last started. */
    private boolean takeRestartRequest() {
        synchronized (mLock) {
            boolean requested = mRestartRequested;
            mRestartRequested = false;
            return requested;
        }
    }

    /** One run of the command, to its end. Returns what to add to the notice about its end: its exit code and last output. */
    private String runOnce() {
        List<String> command;
        try {
            command = mCommands.create(System.currentTimeMillis());
        } catch (RuntimeException e) {
            notice("Could not build the command: " + e);
            return "";
        }
        Process process = null;
        ArrayDeque<String> lastLines = new ArrayDeque<>();
        boolean listenerFailed = false;
        int exit;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
            synchronized (mLock) {
                if (mStopped) {
                    process.destroy();
                    return "";
                }
                mProcess = process;
                mRestartRequested = false;           // this run is a fresh one: a restart asked for before it began is what it is
            }
            notice("Started: " + String.join(" ", command));
            runStarted();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (lastLines.size() == LAST_LINES_KEPT) {
                    lastLines.pollFirst();
                }
                lastLines.addLast(line.length() > LAST_LINE_MAX_CHARS ? line.substring(0, LAST_LINE_MAX_CHARS) : line);
                try {
                    mListener.onLine(line);
                } catch (RuntimeException e) {
                    // A fault in the listener must not end the reading; said once per run, not once per line.
                    if (!listenerFailed) {
                        listenerFailed = true;
                        notice("The listener failed on a line (more are not reported): " + e);
                    }
                }
            }
        } catch (IOException e) {
            // A stop, or a restart asked for, ends the command on purpose; the read it was blocked in fails because of that.
            if (!isStopped() && !isRestartRequested()) {
                notice("Could not run or read it: " + e);
            }
        } finally {
            exit = reap(process);
            synchronized (mLock) {
                mProcess = null;
            }
        }
        if (exit == Integer.MIN_VALUE || isStopped()) {
            return "";
        }
        return " (exit " + exit + ")" + (lastLines.isEmpty() ? "" : ", last output: " + String.join(" | ", lastLines));
    }

    /** Makes sure the process is gone and collected; its exit code, or Integer.MIN_VALUE if there was no process / it would not end. */
    private static int reap(Process process) {
        if (process == null) {
            return Integer.MIN_VALUE;
        }
        process.destroy();
        try {
            if (!process.waitFor(REAP_MILLIS, TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                if (!process.waitFor(REAP_MILLIS, TimeUnit.MILLISECONDS)) {
                    return Integer.MIN_VALUE;
                }
            }
            return process.exitValue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();       // being stopped
            process.destroyForcibly();
            return Integer.MIN_VALUE;
        }
    }

    private void waitBeforeRetry(long millis) {
        long endNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        synchronized (mLock) {
            while (!mStopped && !mRestartRequested) {
                long leftMillis = TimeUnit.NANOSECONDS.toMillis(endNanos - System.nanoTime());
                if (leftMillis <= 0) {
                    return;
                }
                try {
                    mLock.wait(leftMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void notice(String text) {
        try {
            mListener.onNotice(text);
        } catch (RuntimeException ignored) {
            // Reporting must never end the reading.
        }
    }

    private void runStarted() {
        try {
            mListener.onRunStarted();
        } catch (RuntimeException ignored) {
            // Reporting must never end the reading.
        }
    }

    private void runEnded() {
        try {
            mListener.onRunEnded();
        } catch (RuntimeException ignored) {
            // Reporting must never end the reading.
        }
    }
}
