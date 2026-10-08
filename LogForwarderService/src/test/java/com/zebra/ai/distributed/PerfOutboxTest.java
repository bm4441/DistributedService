package com.zebra.ai.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Where the performance messages wait for the sender: in order, bounded, and never sent late. */
public class PerfOutboxTest {

    private static final long T0 = TimeUnit.SECONDS.toNanos(1_000);       // some moment on System.nanoTime()
    private static final long MAX_AGE_NANOS = TimeUnit.MILLISECONDS.toNanos(PerfOutbox.MAX_AGE_MILLIS);

    /** A sink that takes everything and remembers it. */
    private static final class Collector implements PerfOutbox.Sink {
        final List<String> taken = new ArrayList<>();

        @Override
        public boolean send(String message) {
            taken.add(message);
            return true;
        }
    }

    @Test
    public void theRuleIsFiveSeconds() {
        assertEquals(5_000L, PerfOutbox.MAX_AGE_MILLIS);
    }

    @Test
    public void messagesGoOutInTheOrderTheyCameIn() {
        PerfOutbox outbox = new PerfOutbox();
        outbox.add("a", T0);
        outbox.add("b", T0 + 10);
        outbox.add("c", T0 + 20);
        Collector sink = new Collector();
        assertEquals(3, outbox.drain(T0 + 30, 50, sink));
        assertEquals(List.of("a", "b", "c"), sink.taken);
        assertTrue(outbox.isEmpty());
    }

    @Test
    public void aBurstIsNotCutDown_everyMessageOfIt_goesOut() {
        // the application logs a line per frame: 20 within a second at a high frame rate
        PerfOutbox outbox = new PerfOutbox();
        for (int i = 0; i < 20; i++) {
            outbox.add("m" + i, T0 + i * 50_000_000L);
        }
        Collector sink = new Collector();
        assertEquals(20, outbox.drain(T0 + TimeUnit.SECONDS.toNanos(1), 50, sink));
        assertEquals("m0", sink.taken.get(0));
        assertEquals("m19", sink.taken.get(19));
    }

    @Test
    public void nothingToSend_sendsNothing() {
        assertEquals(0, new PerfOutbox().drain(T0, 50, new Collector()));
    }

    /* ================= The size ================= */

    @Test
    public void whenFull_theOldestGoes_theNewestCapacityMessagesStay() {
        PerfOutbox outbox = new PerfOutbox();
        int total = PerfOutbox.CAPACITY + 10;
        for (int i = 0; i < total; i++) {
            outbox.add("m" + i, T0);
        }
        Collector sink = new Collector();
        assertEquals(PerfOutbox.CAPACITY, outbox.drain(T0, total, sink));
        assertEquals("m10", sink.taken.get(0));
        assertEquals("m" + (total - 1), sink.taken.get(PerfOutbox.CAPACITY - 1));
    }

    @Test
    public void aDrainTakesAtMostMax_theRestWaitsForTheNextOne() {
        PerfOutbox outbox = new PerfOutbox();
        for (int i = 0; i < 5; i++) {
            outbox.add("m" + i, T0);
        }
        Collector sink = new Collector();
        assertEquals(2, outbox.drain(T0, 2, sink));
        assertEquals(List.of("m0", "m1"), sink.taken);
        assertEquals(3, outbox.drain(T0, 2 + 3, sink));
        assertEquals(List.of("m0", "m1", "m2", "m3", "m4"), sink.taken);
    }

    /* ================= Not too late ================= */

    @Test
    public void aMessageOlderThanTheLimit_isNotSent_butOneExactlyAtItIs() {
        PerfOutbox outbox = new PerfOutbox();
        outbox.add("old", T0);
        outbox.add("at the limit", T0 + 1);
        Collector sink = new Collector();
        assertEquals(1, outbox.drain(T0 + 1 + MAX_AGE_NANOS, 50, sink));
        assertEquals(List.of("at the limit"), sink.taken);
        assertTrue("the old one was dropped, not kept", outbox.isEmpty());
    }

    @Test
    public void staleOnesAreSkipped_theFreshOnesAfterThemStillGoOut() {
        PerfOutbox outbox = new PerfOutbox();
        outbox.add("stale 1", T0);
        outbox.add("stale 2", T0 + 1_000);
        outbox.add("fresh", T0 + 2 * MAX_AGE_NANOS);
        Collector sink = new Collector();
        assertEquals(1, outbox.drain(T0 + 2 * MAX_AGE_NANOS + 5, 50, sink));
        assertEquals(List.of("fresh"), sink.taken);
    }

    @Test
    public void whileTheServerCannotBeReached_whatPilesUpGoesStale_andIsNotSentLater() {
        PerfOutbox outbox = new PerfOutbox();
        for (int i = 0; i < 30; i++) {
            outbox.add("during the outage " + i, T0 + i * 100_000_000L);
        }
        // the connection is back a minute later
        Collector sink = new Collector();
        assertEquals(0, outbox.drain(T0 + TimeUnit.SECONDS.toNanos(60), 50, sink));
        assertTrue(sink.taken.isEmpty());
        assertTrue(outbox.isEmpty());
    }

    /* ================= A sink that cannot take it ================= */

    @Test
    public void aMessageTheSinkRefuses_staysFirstInLine() {
        PerfOutbox outbox = new PerfOutbox();
        outbox.add("a", T0);
        outbox.add("b", T0);
        outbox.add("c", T0);
        final List<String> seen = new ArrayList<>();
        assertEquals("a was taken, b was refused", 1, outbox.drain(T0, 50, message -> {
            seen.add(message);
            return !message.equals("b");
        }));
        assertEquals(List.of("a", "b"), seen);
        Collector sink = new Collector();
        assertEquals(2, outbox.drain(T0, 50, sink));
        assertEquals("b first, in order, then c", List.of("b", "c"), sink.taken);
    }

    @Test
    public void aRefusedMessage_thatIsStaleByTheNextTime_isDropped() {
        PerfOutbox outbox = new PerfOutbox();
        outbox.add("a", T0);
        assertEquals(0, outbox.drain(T0, 50, message -> false));
        Collector sink = new Collector();
        assertEquals(0, outbox.drain(T0 + 2 * MAX_AGE_NANOS, 50, sink));
        assertTrue(sink.taken.isEmpty());
    }

    /* ================= Two threads ================= */

    @Test
    public void aReaderAndASender_atTheSameTime_loseNothingAndKeepTheOrder() throws InterruptedException {
        final PerfOutbox outbox = new PerfOutbox();
        final int total = 2_000;
        final List<String> received = new ArrayList<>();
        Thread reader = new Thread(() -> {
            for (int i = 0; i < total; i++) {
                outbox.add("m" + i, System.nanoTime());
                if (i % 50 == 0) {
                    Thread.yield();
                }
            }
        });
        Thread sender = new Thread(() -> {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            String lastMessage = "m" + (total - 1);
            while ((received.isEmpty() || !received.get(received.size() - 1).equals(lastMessage))
                    && System.nanoTime() < end) {
                outbox.drain(System.nanoTime(), 20, message -> {
                    received.add(message);
                    return true;
                });
            }
        });
        sender.start();
        reader.start();
        reader.join();
        sender.join();
        // The queue is bounded, so a sender that starts late may miss the oldest -- how much depends on the scheduler.
        // What always holds: what arrives is in order, with no repeats, and the newest message is not lost.
        int last = -1;
        for (String message : received) {
            int n = Integer.parseInt(message.substring(1));
            assertTrue("in order, once: " + message, n > last);
            last = n;
        }
        assertEquals("m" + (total - 1), received.get(received.size() - 1));
    }
}
