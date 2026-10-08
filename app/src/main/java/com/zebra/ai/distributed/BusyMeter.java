package com.zebra.ai.distributed;

/**
 * How much of the time the AI accelerator path (the DSP/NPU, or whatever the AI Vision SDK runs on)
 * is occupied by this service: the share of a sampling window during which at least one model
 * warm-up or inference was running. Android gives an ordinary app no reading of the real DSP/NPU
 * load, but this service is the only thing on the device that drives these models, so its own duty
 * cycle is the honest figure it can offer.
 *
 * Code wraps each warm-up/inference in begin()/end(); overlapping calls count once. The monitor
 * calls sampleBusyPercent() every few seconds. All methods are thread-safe. The time arguments
 * (any steadily increasing clock, in nanoseconds) are passed in so the tests can drive it.
 */
final class BusyMeter {

    private int mRunning;                 // begin() calls not yet matched by end()
    private long mRunningSinceNanos;      // start of the current busy stretch (valid while mRunning > 0)
    private long mBusyNanos;              // busy time of finished stretches
    private long mLastBusyNanos;          // total busy time at the previous sample
    private long mLastSampleNanos = -1;
    private long mCompletedCount;
    private long mLastDurationMillis = -1;

    void begin() {
        begin(System.nanoTime());
    }

    void end() {
        end(System.nanoTime());
    }

    synchronized void begin(long nowNanos) {
        if (mRunning++ == 0) {
            mRunningSinceNanos = nowNanos;
        }
    }

    synchronized void end(long nowNanos) {
        if (mRunning == 0) {
            return;                       // unmatched end(): ignore rather than go negative
        }
        if (--mRunning == 0) {
            mBusyNanos += nowNanos - mRunningSinceNanos;
        }
    }

    /** Records one finished task for the counters (see completedCount()/lastDurationMillis()). */
    synchronized void taskDone(long durationMillis) {
        mCompletedCount++;
        mLastDurationMillis = durationMillis;
    }

    synchronized long completedCount() {
        return mCompletedCount;
    }

    /** Duration of the most recent finished task in ms, or -1 if none yet. */
    synchronized long lastDurationMillis() {
        return mLastDurationMillis;
    }

    /** Busy share (0-100) of the time since the previous call; NaN for the first call. */
    synchronized double sampleBusyPercent(long nowNanos) {
        long busyNow = mBusyNanos + (mRunning > 0 ? nowNanos - mRunningSinceNanos : 0);
        double percent = Double.NaN;
        if (mLastSampleNanos >= 0 && nowNanos > mLastSampleNanos) {
            percent = 100.0 * (busyNow - mLastBusyNanos) / (nowNanos - mLastSampleNanos);
            percent = Math.max(0.0, Math.min(100.0, percent));
        }
        mLastBusyNanos = busyNow;
        mLastSampleNanos = nowNanos;
        return percent;
    }
}
