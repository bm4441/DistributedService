package com.zebra.ai.distributed;

import android.os.Process;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory for this service's own single-thread executors -- each peer's transfer thread
 * (PeerTransferWorker), the task-processing thread (TaskTransferEngine) and the result-transfer
 * thread (ResultTransferEngine) -- that logs each thread's whole life: when it's created, its
 * Linux thread id once it's running (the TID column in logcat, so every later line from that
 * thread can be matched to its name), and when and how it exits. A single-thread executor only
 * creates a second thread after the first one has died, so a "#2" thread in the log means the
 * previous one was lost to an uncaught exception.
 */
final class LoggingThreadFactory implements ThreadFactory {

    private static final String TAG = "DistributedService";

    private final String mName;
    private final AtomicInteger mCreatedCount = new AtomicInteger(0);

    LoggingThreadFactory(String name) {
        mName = name;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        int count = mCreatedCount.incrementAndGet();
        String threadName = count == 1 ? mName : mName + "#" + count;
        if (count == 1) {
            EventLog.i(TAG, "Creating thread " + threadName);
        } else {
            EventLog.w(TAG, "Creating thread " + threadName + " to replace one that exited unexpectedly");
        }
        return new Thread(() -> {
            EventLog.i(TAG, "Thread " + threadName + " running (tid=" + Process.myTid() + ")");
            boolean returnedNormally = false;
            try {
                runnable.run();
                returnedNormally = true;
            } finally {
                if (returnedNormally) {
                    EventLog.i(TAG, "Thread " + threadName + " (tid=" + Process.myTid() + ") exited");
                } else {
                    // The exception itself is logged by DistributedService's crash logger.
                    EventLog.e(TAG, "Thread " + threadName + " (tid=" + Process.myTid()
                            + ") is exiting on an uncaught exception");
                }
            }
        }, threadName);
    }
}
