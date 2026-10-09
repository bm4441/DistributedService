package com.zebra.ai.distributed;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import io.grpc.ConnectivityState;

/**
 * Notices a contributor that drops while this device (the requestor) is idle towards it -- nothing is being sent to it, so
 * no send fails to say so. Every POLL_MILLIS it looks at the gRPC connection to each contributor it is watching (the ones
 * that were sent a ready check, see PeerReadinessEngine.sendReadyCheck()); one whose connection has been down for
 * DOWN_FOR_LOST_MILLIS is reported lost. A contributor that vanishes without closing its connection (its Wi-Fi switched
 * off, say) is noticed by gRPC's keepalive pings first (see GrpcTransportEngine), which takes about half a minute.
 *
 * While this device has no Wi-Fi itself nothing is blamed on the contributors: the clock of every connection restarts
 * when Wi-Fi is back. A peer is watched until it is reported, or the session ends (clear()).
 *
 * The watching runs on the service's scheduler, and only while there is something to watch.
 */
final class PeerLossWatcher {

    static final long POLL_MILLIS = 1000;
    /** A connection must be down this long before the peer is reported lost: a reconnect after a blip is not a loss. */
    static final long DOWN_FOR_LOST_MILLIS = 5000;

    /** What the watcher needs from the service. */
    interface Host {
        /** The state of the gRPC connection to this address (it asks the channel to connect if it is idle); null if there is none. */
        ConnectivityState connectionState(String hostAddress);

        /** True while this device has Wi-Fi. */
        boolean isWifiUp();

        /** The peer's connection has been down long enough: it is lost. */
        void peerLost(String peerId, String why);
    }

    private static final class Watched {
        final String hostAddress;
        long downSinceMillis = -1;          // -1 while the connection is up (or cannot be judged)

        Watched(String hostAddress) {
            this.hostAddress = hostAddress;
        }
    }

    private final Host mHost;
    private final ScheduledExecutorService mScheduler;
    private final LongSupplier mClockMillis;
    private final Map<String, Watched> mWatched = new ConcurrentHashMap<>();
    private ScheduledFuture<?> mTask;        // guarded by this

    PeerLossWatcher(Host host, ScheduledExecutorService scheduler, LongSupplier clockMillis) {
        mHost = host;
        mScheduler = scheduler;
        mClockMillis = clockMillis;
    }

    /** Starts watching a peer's connection (does nothing if it is watched already). */
    synchronized void watch(String peerId, String hostAddress) {
        if (peerId == null || hostAddress == null || hostAddress.isEmpty()) {
            return;
        }
        mWatched.putIfAbsent(peerId, new Watched(hostAddress));
        if (mTask == null) {
            try {
                mTask = mScheduler.scheduleWithFixedDelay(this::tickSafely, POLL_MILLIS, POLL_MILLIS, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                mWatched.clear();                // the service is shutting down
            }
        }
    }

    void unwatch(String peerId) {
        mWatched.remove(peerId);
    }

    /** Stops watching everybody -- the session is over. */
    void clear() {
        mWatched.clear();
    }

    int watchedCount() {
        return mWatched.size();
    }

    private void tickSafely() {
        try {
            tick(mClockMillis.getAsLong());
        } catch (RuntimeException e) {
            // A task on the scheduler that throws is never run again, and the peers would go unwatched.
            EventLog.e("DistributedService", "The peer connection watcher failed; continuing", e);
        }
    }

    /** One look at every watched connection. Package-visible for the tests, which pass the time in. */
    void tick(long nowMillis) {
        if (mWatched.isEmpty()) {
            synchronized (this) {
                if (mWatched.isEmpty() && mTask != null) {      // nothing to watch: stop waking up
                    mTask.cancel(false);
                    mTask = null;
                }
            }
            return;
        }
        List<String[]> lost = new ArrayList<>();
        boolean wifiUp = mHost.isWifiUp();
        Iterator<Map.Entry<String, Watched>> it = mWatched.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Watched> entry = it.next();
            Watched peer = entry.getValue();
            ConnectivityState state = wifiUp ? mHost.connectionState(peer.hostAddress) : null;
            if (state == null || state == ConnectivityState.READY) {
                peer.downSinceMillis = -1;       // up -- or nothing to judge it by (no Wi-Fi of our own, no connection)
                continue;
            }
            if (peer.downSinceMillis < 0) {
                peer.downSinceMillis = nowMillis;
                continue;
            }
            long downMillis = nowMillis - peer.downSinceMillis;
            if (downMillis >= DOWN_FOR_LOST_MILLIS) {
                it.remove();
                lost.add(new String[]{entry.getKey(), "its connection has been down for " + downMillis / 1000 + " s (" + state + ")"});
            }
        }
        for (String[] peer : lost) {             // reported after the pass: the application is called, which may take a moment
            mHost.peerLost(peer[0], peer[1]);
        }
    }
}
