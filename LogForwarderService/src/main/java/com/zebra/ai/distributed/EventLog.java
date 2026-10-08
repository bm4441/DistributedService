package com.zebra.ai.distributed;

import android.app.Application;
import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.ServiceConnection;
import android.os.IBinder;
import android.os.Process;
import android.os.RemoteException;
import android.os.SystemClock;
import android.util.Log;

import androidx.core.content.ContextCompat;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Drop-in replacement for android.util.Log inside the service process (same v/d/i/w/e methods):
 * every log call becomes an "event" that is queued here and sent, in batches, to the log
 * forwarder process (LogForwarderService), which streams it to the dashboard server -- so the
 * threads that log never wait on logcat or the network.
 *
 * Two settings, changed live from the UI (see LogConfig), decide what each call does:
 *  - Log Print: also write the line to logcat, at every level, on the calling thread.
 *  - Event Level: only calls at or above it are queued for the server; lower ones are dropped
 *    right here. This never limits what Log Print writes to logcat.
 * A call that neither prints nor sends returns immediately; isEnabled() lets very hot call sites
 * skip even building their message in that case.
 *
 * The queue is bounded: if the forwarder is slow or not connected, the oldest events are
 * dropped (and a warning event reports how many), never the caller blocked.
 */
final class EventLog {

    private static final String TAG = "EventLog";

    private static final int MAX_QUEUED_EVENTS = 10_000;
    /** Per batch: whichever limit is hit first. 96 KB stays far below Binder's transaction limit. */
    private static final int MAX_BATCH_EVENTS = 200;
    private static final int MAX_BATCH_BYTES = 96 * 1024;
    private static final int MAX_MESSAGE_CHARS = 8_000;
    /** After the first event of a burst arrives, wait this long so the rest joins the same batch. */
    private static final long BATCH_WINDOW_MILLIS = 100;
    private static final long IDLE_POLL_MILLIS = 500;
    private static final long SINK_WAIT_MILLIS = 250;
    private static final long RETRY_DELAY_MILLIS = 500;
    /** Consecutive failed sends of one batch before it is given up on. */
    private static final int MAX_SEND_FAILURES = 20;
    private static final long FLUSH_TIMEOUT_MILLIS = 500;
    private static final long SENDER_JOIN_MILLIS = 300;

    private static final LinkedBlockingDeque<LogEvent> sQueue = new LinkedBlockingDeque<>(MAX_QUEUED_EVENTS);
    private static final AtomicLong sDropped = new AtomicLong();
    private static final int sPid = Process.myPid();

    // The defaults match LogConfig's, so logging before init() behaves like the saved defaults.
    private static volatile boolean sLogPrint = LogConfig.DEFAULT_LOG_PRINT;
    private static volatile int sSendLevel = LogConfig.DEFAULT_EVENT_LEVEL;
    /** The forwarder's end of the connection; null while it isn't connected. */
    private static volatile LogSink.Client sSink;
    private static volatile boolean sRunning;
    /** The newest device-status message (see sendDeviceStatus()), kept for a forwarder that connects later. */
    private static volatile String sLastDeviceStatus;

    private static final Object sLifecycleLock = new Object();
    private static boolean sReceiverRegistered;   // guarded by sLifecycleLock
    private static Context sBoundContext;         // guarded by sLifecycleLock
    private static Thread sSenderThread;          // guarded by sLifecycleLock

    private EventLog() {
    }

    /* ================= The android.util.Log-compatible API ================= */

    static void v(String tag, String msg) {
        log(Log.VERBOSE, tag, msg, null);
    }

    static void v(String tag, String msg, Throwable t) {
        log(Log.VERBOSE, tag, msg, t);
    }

    static void d(String tag, String msg) {
        log(Log.DEBUG, tag, msg, null);
    }

    static void d(String tag, String msg, Throwable t) {
        log(Log.DEBUG, tag, msg, t);
    }

    static void i(String tag, String msg) {
        log(Log.INFO, tag, msg, null);
    }

    static void i(String tag, String msg, Throwable t) {
        log(Log.INFO, tag, msg, t);
    }

    static void w(String tag, String msg) {
        log(Log.WARN, tag, msg, null);
    }

    static void w(String tag, String msg, Throwable t) {
        log(Log.WARN, tag, msg, t);
    }

    static void e(String tag, String msg) {
        log(Log.ERROR, tag, msg, null);
    }

    static void e(String tag, String msg, Throwable t) {
        log(Log.ERROR, tag, msg, t);
    }

    /** True if a call at this level would do anything (print to logcat or queue for the server). */
    static boolean isEnabled(int level) {
        return sLogPrint || level >= sSendLevel;
    }

    /** For call sites that fire per chunk/message: skip building the text when nothing wants it. */
    static boolean isDebugEnabled() {
        return isEnabled(Log.DEBUG);
    }

    private static void log(int level, String tag, String msg, Throwable t) {
        boolean print = sLogPrint;
        boolean send = level >= sSendLevel;
        if (!print && !send) {
            return;
        }
        String text = t == null ? String.valueOf(msg) : msg + "\n" + Log.getStackTraceString(t);
        if (print) {
            Log.println(level, tag, text);
        }
        if (send) {
            offer(newEvent(level, tag, text));
        }
    }

    /**
     * Hands one device-status message (JSON built by DeviceStatus.toJson(): battery, memory, CPU,
     * accelerator load, the service policy) to the forwarder, which sends it to the server next to
     * the log lines. Not a log event: it bypasses the queue and the Event Level -- only the newest
     * one matters, so one that can't be delivered right now is simply replaced by the next. Never
     * blocks (a oneway Binder call).
     */
    static void sendDeviceStatus(String json) {
        sLastDeviceStatus = json;
        LogSink.Client sink = sSink;
        if (sink == null) {
            return;   // the forwarder isn't connected yet: onServiceConnected() sends the latest one
        }
        try {
            sink.onDeviceStatus(json);
        } catch (RemoteException | RuntimeException e) {
            // The forwarder's process is gone; the next one is given the latest message on connecting.
        }
    }

    /**
     * For the uncaught-exception handler, which may be the last thing this process ever does:
     * writes to logcat whatever the Log Print setting, then makes a last attempt to hand
     * everything still queued -- this event included -- to the forwarder before the process dies.
     */
    static void fatal(String tag, String msg, Throwable t) {
        String text = msg + "\n" + Log.getStackTraceString(t);
        Log.println(Log.ERROR, tag, text);
        if (Log.ERROR >= sSendLevel) {
            offer(newEvent(Log.ERROR, tag, text));
        }
        flush(FLUSH_TIMEOUT_MILLIS);
    }

    /* ================= Lifecycle: DistributedService.onCreate() / onDestroy() ================= */

    /**
     * Loads the saved settings, starts the batch sender and binds -- which starts -- the log
     * forwarder process. Safe to call again after shutdown() (the service can be restarted in the
     * same process); calls in between do nothing new.
     */
    static void init(Context context) {
        Context app = context.getApplicationContext();
        applySettings(LogConfig.read(app));
        synchronized (sLifecycleLock) {
            if (!sReceiverRegistered) {
                // Kept for the life of the process, so a settings change reaches us even between
                // two runs of the service.
                ContextCompat.registerReceiver(app, sSettingsReceiver,
                        new IntentFilter(LogConfig.ACTION_SETTINGS_CHANGED),
                        ContextCompat.RECEIVER_NOT_EXPORTED);
                sReceiverRegistered = true;
            }
            if (!sRunning) {
                sRunning = true;
                sSenderThread = new Thread(EventLog::senderLoop, "EventLogSender");
                sSenderThread.setDaemon(true);
                sSenderThread.start();
            }
            if (sBoundContext == null) {
                Intent forwarder = new Intent(context, LogForwarderService.class);
                if (context.bindService(forwarder, sConnection, Context.BIND_AUTO_CREATE)) {
                    sBoundContext = context;
                } else {
                    Log.e(TAG, "Could not bind the log forwarder service; logs will not be forwarded");
                    // Required even when bindService() fails, to release the connection object.
                    context.unbindService(sConnection);
                }
            }
        }
        i(TAG, "Event logging started in process " + Application.getProcessName()
                + " (queue " + MAX_QUEUED_EVENTS + " events, batches of up to " + MAX_BATCH_EVENTS + ")");
    }

    /**
     * Stops the batch sender, makes a last attempt to deliver what is still queued, and unbinds
     * the forwarder -- which then flushes to the server and stops.
     */
    static void shutdown() {
        Thread sender;
        synchronized (sLifecycleLock) {
            sRunning = false;
            sender = sSenderThread;
            sSenderThread = null;
        }
        if (sender != null) {
            sender.interrupt();
            try {
                sender.join(SENDER_JOIN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        flush(FLUSH_TIMEOUT_MILLIS);
        synchronized (sLifecycleLock) {
            if (sBoundContext != null) {
                try {
                    sBoundContext.unbindService(sConnection);
                } catch (IllegalArgumentException ignored) {
                    // Already unbound.
                }
                sBoundContext = null;
            }
        }
        sSink = null;
        sLastDeviceStatus = null;   // a restarted service must not greet its forwarder with the old run's last words
    }

    /* ================= Settings ================= */

    private static void applySettings(LogConfig.Settings settings) {
        boolean changed = sLogPrint != settings.logPrint || sSendLevel != settings.eventLevel;
        sLogPrint = settings.logPrint;
        sSendLevel = settings.eventLevel;
        if (changed) {
            // Straight to logcat, so the change itself is visible whatever it just switched.
            Log.i(TAG, "Log settings: logPrint=" + settings.logPrint + ", send level="
                    + LogConfig.LEVEL_LABELS[LogConfig.levelToIndex(settings.eventLevel)]);
        }
    }

    private static final BroadcastReceiver sSettingsReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            LogConfig.Settings settings = LogConfig.fromIntent(intent);
            if (settings != null) {
                applySettings(settings);
            }
        }
    };

    /* ================= The connection to the forwarder process ================= */

    private static final ServiceConnection sConnection = new ServiceConnection() {
        @Override
        public void onServiceConnected(ComponentName name, IBinder binder) {
            LogSink.Client sink = new LogSink.Client(binder);
            sSink = sink;
            Log.i(TAG, "Connected to the log forwarder; " + sQueue.size() + " queued event(s) will be sent");
            String lastStatus = sLastDeviceStatus;
            if (lastStatus != null) {
                try {
                    sink.onDeviceStatus(lastStatus);
                } catch (RemoteException | RuntimeException ignored) {
                    // The next status message follows within seconds.
                }
            }
        }

        @Override
        public void onServiceDisconnected(ComponentName name) {
            // The forwarder's process died; the system restarts it, since we're still bound, and
            // onServiceConnected() runs again. Events queue up meanwhile.
            sSink = null;
            Log.w(TAG, "Lost the log forwarder; events are queued until it is back");
        }

        @Override
        public void onBindingDied(ComponentName name) {
            sSink = null;
            Log.w(TAG, "The log forwarder binding died; binding again");
            rebind();
        }

        @Override
        public void onNullBinding(ComponentName name) {
            Log.e(TAG, "The log forwarder returned no binder; logs will not be forwarded");
        }
    };

    private static void rebind() {
        synchronized (sLifecycleLock) {
            Context context = sBoundContext;
            if (context == null) {
                return;
            }
            try {
                context.unbindService(sConnection);
            } catch (IllegalArgumentException ignored) {
                // Already unbound.
            }
            if (!context.bindService(new Intent(context, LogForwarderService.class),
                    sConnection, Context.BIND_AUTO_CREATE)) {
                Log.e(TAG, "Could not bind the log forwarder service again");
            }
        }
    }

    /* ================= Queue and batch sender ================= */

    private static LogEvent newEvent(int level, String tag, String text) {
        if (text.length() > MAX_MESSAGE_CHARS) {
            text = text.substring(0, MAX_MESSAGE_CHARS) + "...(truncated)";
        }
        return new LogEvent(System.currentTimeMillis(), level, sPid, Process.myTid(), String.valueOf(tag), text);
    }

    /** Never blocks: if the queue is full the oldest event makes room for this one. */
    private static void offer(LogEvent event) {
        while (!sQueue.offerLast(event)) {
            if (sQueue.pollFirst() != null) {
                sDropped.incrementAndGet();
            }
        }
    }

    /**
     * Moves queued events into out without waiting, up to the batch limits, preceded by a warning
     * if events were dropped since the last batch. Returns the size of out.
     */
    private static int takeBatch(ArrayList<LogEvent> out) {
        int bytes = 0;
        for (LogEvent event : out) {
            bytes += event.estimatedBytes();
        }
        long dropped = sDropped.getAndSet(0);
        if (dropped > 0) {
            LogEvent notice = newEvent(Log.WARN, TAG, "Dropped " + dropped
                    + " log event(s): the queue was full because the log forwarder was not keeping up");
            out.add(notice);
            bytes += notice.estimatedBytes();
        }
        while (out.size() < MAX_BATCH_EVENTS && bytes < MAX_BATCH_BYTES) {
            LogEvent event = sQueue.pollFirst();
            if (event == null) {
                break;
            }
            out.add(event);
            bytes += event.estimatedBytes();
        }
        return out.size();
    }

    /** Runs on its own low-priority thread: queue -> batches -> one oneway Binder call each. */
    private static void senderLoop() {
        Process.setThreadPriority(Process.THREAD_PRIORITY_BACKGROUND);
        ArrayList<LogEvent> pending = new ArrayList<>();   // the batch being sent (or retried)
        int failures = 0;
        try {
            while (sRunning) {
                try {
                    if (pending.isEmpty()) {
                        LogEvent first = sQueue.pollFirst(IDLE_POLL_MILLIS, TimeUnit.MILLISECONDS);
                        if (first != null) {
                            pending.add(first);
                            if (sQueue.size() < MAX_BATCH_EVENTS) {
                                Thread.sleep(BATCH_WINDOW_MILLIS);   // let the rest of the burst arrive
                            }
                        } else if (sDropped.get() == 0) {
                            continue;
                        }
                        takeBatch(pending);
                    }
                    LogSink.Client sink = sSink;
                    if (sink == null) {
                        Thread.sleep(SINK_WAIT_MILLIS);   // forwarder not connected yet; keep queueing
                        continue;
                    }
                    try {
                        // The batch is copied into the Parcel before this returns, so pending can be reused.
                        sink.onEvents(pending);
                        pending.clear();
                        failures = 0;
                    } catch (RemoteException | RuntimeException e) {
                        if (++failures >= MAX_SEND_FAILURES) {
                            Log.w(TAG, "Giving up on a batch of " + pending.size()
                                    + " log event(s): the log forwarder did not accept it", e);
                            pending.clear();
                            failures = 0;
                        } else {
                            Thread.sleep(RETRY_DELAY_MILLIS);
                        }
                    }
                } catch (RuntimeException e) {
                    // A bug here must not end logging for the rest of the process's life.
                    Log.e(TAG, "Unexpected error in the log sender; dropping its batch and continuing", e);
                    pending.clear();
                    Thread.sleep(RETRY_DELAY_MILLIS);
                }
            }
        } catch (InterruptedException e) {
            // shutdown()
        } finally {
            // Whatever wasn't sent goes back to the front of the queue, for shutdown()'s flush.
            for (int index = pending.size() - 1; index >= 0; index--) {
                sQueue.offerFirst(pending.get(index));
            }
        }
    }

    /**
     * Sends whatever is queued right now from the calling thread, until the queue is empty or the
     * time is up. Only for shutdown and a crash, when the sender thread can't be relied on.
     */
    private static void flush(long timeoutMillis) {
        long deadline = SystemClock.elapsedRealtime() + timeoutMillis;
        ArrayList<LogEvent> batch = new ArrayList<>();
        while (SystemClock.elapsedRealtime() < deadline) {
            LogSink.Client sink = sSink;
            if (sink == null) {
                return;
            }
            batch.clear();
            if (takeBatch(batch) == 0) {
                return;
            }
            try {
                sink.onEvents(batch);
            } catch (RemoteException | RuntimeException e) {
                return;
            }
        }
    }
}
