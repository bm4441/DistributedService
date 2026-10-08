package com.zebra.ai.distributed;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Where the performance messages (see PerformanceMetric) wait between the thread that reads them from logcat and
 * the sender thread that writes them to the server. The demo application logs a line per processed frame -- a
 * burst of several within a few hundred milliseconds at a high frame rate, then one a second or less -- and the
 * sender looks in a few times a second, so this is a small queue, not a single slot: nothing is lost in a burst.
 *
 * It is bounded (the oldest message goes when it is full), and a message that has waited longer than
 * MAX_AGE_MILLIS is not sent at all: these are live readings, and one that was read while the server could not
 * be reached would only show a state that is long past (the next reading follows within a second if the
 * application is still running).
 *
 * Plain Java, times passed in, thread-safe: it runs in plain unit tests.
 */
final class PerfOutbox {

    static final int CAPACITY = 256;
    static final long MAX_AGE_MILLIS = 5_000L;

    /** Where a message goes: true if it was taken (written to the connection), false if it could not be -- it stays. */
    interface Sink {
        boolean send(String message);
    }

    private static final class Item {
        final String message;
        final long atNanos;

        Item(String message, long atNanos) {
            this.message = message;
            this.atNanos = atNanos;
        }
    }

    private final LinkedBlockingDeque<Item> mItems = new LinkedBlockingDeque<>(CAPACITY);

    /** Queues a message that was read at nowNanos (System.nanoTime()); when the queue is full the oldest goes. */
    void add(String message, long nowNanos) {
        Item item = new Item(message, nowNanos);
        while (!mItems.offerLast(item)) {
            mItems.pollFirst();
        }
    }

    /**
     * Gives the sink, oldest first, the messages that are not too old as of nowNanos -- the too old ones are dropped
     * -- until it refuses one (that one stays at the front for the next time) or max have been taken. Returns how
     * many the sink took.
     */
    int drain(long nowNanos, int max, Sink sink) {
        long maxAgeNanos = TimeUnit.MILLISECONDS.toNanos(MAX_AGE_MILLIS);
        int taken = 0;
        while (taken < max) {
            Item item = mItems.pollFirst();
            if (item == null) {
                break;
            }
            if (nowNanos - item.atNanos > maxAgeNanos) {
                continue;
            }
            if (!sink.send(item.message)) {
                mItems.offerFirst(item);           // not taken: first in line next time (dropped if the queue filled up meanwhile)
                break;
            }
            taken++;
        }
        return taken;
    }

    boolean isEmpty() {
        return mItems.isEmpty();
    }
}
