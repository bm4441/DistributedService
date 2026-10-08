package com.zebra.ai.distributed;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Whether Android lets the log forwarder's logcat reader see the demo application's log lines -- the one thing logcat
 * itself never says.
 *
 * Since Android 12 every logcat reader an app starts has to be approved (LogcatManagerService), even when the app holds
 * READ_LOGS: the system asks the user -- "Allow one-time access" -- but only while the app is on screen. A request made
 * at any other moment is declined at once, with no dialog, and logd then gives that reader only the app's own lines:
 * no error, no end, the command just stays quiet (measured on the TC701L; the code is in AOSP's LogcatManagerService
 * and logd). An approval lasts as long as that one reader lives.
 *
 * There is a way to tell, though. logd writes a line of its own for every reader that connects ("logdr: UID=10304
 * ..."), and a reader that was let in is given other uids' lines while one that was not is given only its own -- so if
 * the reader is also asked for the tag "logd" (LOGD_FILTER), that line reaches it when, and only when, it was let in.
 * Any line of logd's, or of the demo application's, is therefore the proof; the lack of one is not (the user may not
 * have answered yet), so after a few seconds it is reported as "waiting" and not as "refused".
 *
 * Plain Java, no Android; the time is passed in, so it can be tested. Safe to use from several threads.
 */
final class LogAccess {

    enum State {
        /** The performance log is switched off: nothing is read. */
        OFF,
        /** The app does not hold READ_LOGS, so Android does not even ask: logcat shows the app only its own lines. */
        NEEDS_PERMISSION,
        /** A reader has just been started (or is about to be): Android is deciding, or asking the user. */
        STARTING,
        /** No proof yet, a while after the reader started: not answered, declined, or asked while the app was not on screen. */
        WAITING,
        /** Proof received: the reader is given other apps' lines. */
        GRANTED
    }

    /** What to add to the reader's tag filters so logd's own lines reach it (see above). */
    static final String LOGD_FILTER = "logd:I";

    /** How long a reader that has shown no proof is still said to be "starting". */
    static final long DECISION_NANOS = 3_000_000_000L;

    /**
     * How long after a reader was started the next one should not be. Android keeps its answer to a request for a minute
     * -- a request made in that time is answered the same way, without asking -- and a request still unanswered after
     * 70 s (debuggable builds) is declined by a timer that is not cancelled when the user answers sooner: so a request
     * made just before that timer fires is declined, and the user's "Allow" for it then has nothing left to approve
     * (seen on the TC501W: asking again 69.7 s after the last request, twice). Past this, neither can happen.
     */
    static final long ASK_AGAIN_NANOS = 75_000_000_000L;

    /**
     * A line of logd's, in the formats logcat prints: brief ("I/logd    (  594): text"), threadtime
     * ("10-07 23:34:23.735   594   594 I logd    : text") and time ("10-07 23:34:23.735 I/logd( 594): text").
     * The tag has to be exactly "logd".
     */
    private static final Pattern LOGD_LINE = Pattern.compile(
            "^(?:[VDIWEFS]/logd\\s*\\(\\s*\\d+\\):"
            + "|\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.\\d+\\s+\\d+\\s+\\d+ [VDIWEFS] logd\\s*:"
            + "|\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d\\.\\d+ [VDIWEFS]/logd\\s*\\(\\s*\\d+\\):)");

    private final Object mLock = new Object();
    private boolean mEnabled;
    private boolean mPermitted = true;
    private boolean mRunning;        // a reader has been started and has not ended
    private boolean mEnded;          // the last reader ended by itself and none runs now
    private boolean mEverStarted;    // mRunStartedNanos is the time the latest reader was started
    private long mRunStartedNanos;
    private boolean mGranted;
    private long mPerfLines;
    private long mLastPerfLineNanos;

    /** True if the line is one of logd's -- a line only a reader that was let in is given. */
    static boolean isLogdLine(String line) {
        return line != null && LOGD_LINE.matcher(line).find();
    }

    /** The state with this name (as ACTION_FORWARDER_STATE carries it); OFF if there is none. */
    static State parse(String name) {
        if (name != null) {
            try {
                return State.valueOf(name.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ignored) {
                // Not one of ours.
            }
        }
        return State.OFF;
    }

    /** Whether the performance log is switched on. Switching it off forgets everything that was learned. */
    void setEnabled(boolean enabled) {
        synchronized (mLock) {
            if (mEnabled == enabled) {
                return;
            }
            mEnabled = enabled;
            if (!enabled) {
                mRunning = false;
                mEnded = false;
                mGranted = false;
                mPerfLines = 0;
            }
        }
    }

    /** Whether the app holds READ_LOGS. */
    void setPermitted(boolean permitted) {
        synchronized (mLock) {
            mPermitted = permitted;
        }
    }

    /** A reader has been started: whatever the one before had been given is gone, and the new one has to be let in. */
    void onRunStarted(long nowNanos) {
        synchronized (mLock) {
            mRunning = true;
            mEnded = false;
            mGranted = false;
            mEverStarted = true;
            mRunStartedNanos = nowNanos;
        }
    }

    /**
     * How long, in milliseconds, until another request for log access can be made (see ASK_AGAIN_NANOS): 0 if one can be
     * made now. Counted from the start of the latest reader, however it came to be started.
     */
    long askAgainInMillis(long nowNanos) {
        synchronized (mLock) {
            if (!mEverStarted) {
                return 0;
            }
            long leftNanos = ASK_AGAIN_NANOS - (nowNanos - mRunStartedNanos);
            return leftNanos <= 0 ? 0 : (leftNanos + 999_999L) / 1_000_000L;
        }
    }

    /** The reader ended by itself (a stop on purpose is setEnabled(false), or the end of the process). */
    void onRunEnded() {
        synchronized (mLock) {
            mRunning = false;
            mEnded = true;
            mGranted = false;
        }
    }

    /** A line of logd's arrived. True if that is what showed the reader was let in. */
    boolean onProof() {
        synchronized (mLock) {
            boolean first = !mGranted;
            mGranted = true;
            return first;
        }
    }

    /** A line of the demo application's arrived (that proves it too). True if that is what showed the reader was let in. */
    boolean onPerfLine(long nowNanos) {
        synchronized (mLock) {
            mPerfLines++;
            mLastPerfLineNanos = nowNanos;
        }
        return onProof();
    }

    State state(long nowNanos) {
        synchronized (mLock) {
            if (!mEnabled) {
                return State.OFF;
            }
            if (!mPermitted) {
                return State.NEEDS_PERMISSION;
            }
            if (mGranted) {
                return State.GRANTED;
            }
            if (mEnded) {
                return State.WAITING;
            }
            if (!mRunning) {
                return State.STARTING;
            }
            return nowNanos - mRunStartedNanos < DECISION_NANOS ? State.STARTING : State.WAITING;
        }
    }

    /** How many lines of the demo application's have been read since the performance log was switched on. */
    long perfLines() {
        synchronized (mLock) {
            return mPerfLines;
        }
    }

    /** How long ago the last line of the demo application's arrived, in milliseconds; -1 if none has. */
    long lastPerfLineAgeMillis(long nowNanos) {
        synchronized (mLock) {
            return mPerfLines == 0 ? -1 : Math.max(0, (nowNanos - mLastPerfLineNanos) / 1_000_000L);
        }
    }
}
