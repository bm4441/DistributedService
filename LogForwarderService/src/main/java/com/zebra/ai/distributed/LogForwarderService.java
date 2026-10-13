package com.zebra.ai.distributed;

import android.Manifest;
import android.app.Service;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.IBinder;
import android.os.Process;
import android.util.Log;

import androidx.annotation.Nullable;
import androidx.core.content.ContextCompat;

import org.json.JSONException;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;

/**
 * Runs in its own process (":log_forwarder"), bound -- and so started -- by DistributedService
 * when it starts (see EventLog.init()). Receives the service's log events over Binder
 * (see LogSink), keeps them in a bounded buffer, and streams them to the dashboard server over
 * a WebSocket, one JSON message per log line:
 * {"type":"log","source":"DistributedService","line":"<logcat -v threadtime style line>"}.
 *
 * On the same WebSocket it also forwards the device's status -- battery, available memory, CPU
 * and AI-accelerator load, and the service policy with its current verdict -- as a message of its
 * own: {"type":"device","source":"DistributedService",...} (see DeviceStatus.toJson()). The
 * service sends one every few seconds and whenever the policy changes. Only the newest matters, so
 * unlike log lines it is not queued: a newer message replaces one that has not gone out yet, and
 * the latest one is sent as soon as a connection to the server is (re)established. As it passes
 * through here, this process adds the device's product name -- the system property ro.product.name,
 * "TC501W" -- as a "product" member next to the "deviceId" the message already has (see ProductName).
 *
 * When the "Performance log" setting is on (see LogConfig; the screen has the switch) it also follows logcat for one
 * tag, "PerformanceMetrics", under which the demo application logs a line per processed frame ("mode=LOCAL
 * capability=ocr.recognize fps=1.38 peers=4/10 lastMs=1093"), and sends the mode, the frame rate, the last processing
 * time and the capability of each as a message of its own:
 * {"type":"perf","source":"PerformanceMetrics","mode":"LOCAL","fps":1.38,"lastMs":1093,"capability":"ocr.recognize"} (see
 * PerformanceMetric, PerfOutbox, LogcatTail). Those lines belong to another application, so reading them needs the
 * READ_LOGS permission -- a normal app only gets it from adb ("adb shell pm grant <package>
 * android.permission.READ_LOGS") -- and, since Android 12, the user's approval of each logcat reader: a question
 * Android asks only while the app is on screen, and a reader started at any other moment never gets it (it is given
 * only the app's own lines, without an error). So the reader is started when the setting is switched on, and again
 * when the screen asks for it (ACTION_PERF_LOG_ACCESS_REQUEST, sent by a tap). Whether Android let it in is worked
 * out (see LogAccess) and reported to the screen with the rest of the forwarder's state.
 *
 * Everything slow -- connecting, retrying, formatting, the network itself -- happens here, never
 * in the service process. While the server is unreachable, events wait in the buffer and are sent
 * once it is back (the oldest are dropped if the buffer fills). This process's own diagnostics go
 * straight to logcat under the tag "LogForwarder", not through EventLog.
 *
 * Stops when the service unbinds: it first gives the last events a moment to reach the server.
 */
public class LogForwarderService extends Service {

    private static final String TAG = "LogForwarder";
    private static final String SOURCE_NAME = "DistributedService";

    private static final int MAX_BUFFERED_EVENTS = 20_000;
    private static final long RECONNECT_DELAY_SECONDS = 5;
    private static final long PING_INTERVAL_SECONDS = 20;
    /** Pause sending while this much is already waiting in the WebSocket's own send queue. */
    private static final long MAX_SOCKET_QUEUE_BYTES = 256 * 1024;
    private static final long SOCKET_BUSY_PAUSE_MILLIS = 20;
    private static final long IDLE_POLL_MILLIS = 200;
    private static final long CONNECTION_WAIT_MILLIS = 500;
    /** On stop: wait up to this long for the buffer to drain, once no new events arrive for QUIET. */
    private static final long STOP_FLUSH_MILLIS = 1000;
    private static final long STOP_QUIET_MILLIS = 150;
    private static final long STOP_POLL_MILLIS = 20;
    private static final long SENDER_JOIN_MILLIS = 300;
    /** At most this many performance messages go out between two looks at the log events and the status. */
    private static final int PERF_MESSAGES_PER_TURN = 50;

    private final LinkedBlockingDeque<LogEvent> mEvents = new LinkedBlockingDeque<>(MAX_BUFFERED_EVENTS);
    private final AtomicLong mDropped = new AtomicLong();
    private volatile long mLastEventNanos = System.nanoTime();
    /** The newest device-status message from the service, and whether it still has to be sent. */
    private volatile String mLatestStatus;
    private final AtomicBoolean mStatusPending = new AtomicBoolean();
    /** This device's product name (system property ro.product.name, "TC501W"); null if the system gave none. Set in onCreate(). */
    private volatile String mProduct;
    /** The performance messages read from logcat that are waiting to be sent -- see PerfOutbox. */
    private final PerfOutbox mPerfOutbox = new PerfOutbox();
    /** The reader of the demo application's log lines; null while the performance log is switched off. Main thread only. */
    private LogcatTail mPerfTail;
    /** The performance log is switched on (LogConfig.Settings.perfLog). Main thread only. */
    private boolean mPerfLogEnabled;
    /** Whether Android lets that reader see other apps' lines (see LogAccess); read by whoever publishes the state. */
    private final LogAccess mLogAccess = new LogAccess();
    // Used by the logcat reading thread; reset on the main thread when a new reader is made.
    private volatile boolean mPerfLineSeen;
    private volatile boolean mOddLineReported;

    private final ScheduledExecutorService mScheduler = Executors.newSingleThreadScheduledExecutor();
    private OkHttpClient mHttpClient;
    private Thread mSenderThread;
    private volatile boolean mRunning;

    /** Guards everything below: the server address and the connection's state. */
    private final Object mConnLock = new Object();
    private String mServerUrl;
    /** Bumped on every attempt and every address change, so callbacks of an old one are ignored. */
    private long mGeneration;
    private boolean mAttemptInFlight;
    private WebSocket mWebSocket;    // non-null only while the connection is open
    private WebSocket mLatest;       // the newest one created, open or not -- to cancel on an address change

    // Used only by the sender thread.
    private final SimpleDateFormat mTimeFormat = new SimpleDateFormat("MM-dd HH:mm:ss.SSS", Locale.US);

    private final IBinder mBinder = new LogSink.Server(new LogSink.Receiver() {
        @Override
        public void onEvents(List<LogEvent> events) {
            bufferEvents(events);
        }

        @Override
        public void onDeviceStatus(String json) {
            bufferStatus(json);
        }
    });

    /** Binder thread: only queue the batch -- sending it is the sender thread's job. */
    private void bufferEvents(List<LogEvent> events) {
        for (LogEvent event : events) {
            while (!mEvents.offerLast(event)) {
                if (mEvents.pollFirst() != null) {
                    mDropped.incrementAndGet();
                }
            }
        }
        mLastEventNanos = System.nanoTime();
    }

    /**
     * Binder thread: remember the newest status (replacing one not yet sent) -- the sender thread sends it. It goes to
     * the server with this device's product name added next to the device id it already carries (see ProductName).
     */
    private void bufferStatus(String json) {
        mLatestStatus = ProductName.addTo(json, mProduct);
        mStatusPending.set(true);
        mLastEventNanos = System.nanoTime();
    }

    private final BroadcastReceiver mReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            String action = intent.getAction();
            if (LogConfig.ACTION_SETTINGS_CHANGED.equals(action)) {
                LogConfig.Settings settings = LogConfig.fromIntent(intent);
                if (settings != null) {
                    changeServerUrl(settings.serverUrl);
                    applyPerfLog(settings.perfLog);
                }
            } else if (LogConfig.ACTION_FORWARDER_STATE_QUERY.equals(action)) {
                publishState(true);
            } else if (LogConfig.ACTION_PERF_LOG_ACCESS_REQUEST.equals(action)) {
                requestPerfLogAccess();
            }
        }
    };

    @Override
    public void onCreate() {
        super.onCreate();
        LogConfig.Settings settings = LogConfig.read(this);
        mServerUrl = settings.serverUrl;
        // Build.PRODUCT is the system property ro.product.name; "unknown" is what Build gives when there is none.
        mProduct = Build.UNKNOWN.equals(Build.PRODUCT) ? null : Build.PRODUCT;
        mHttpClient = new OkHttpClient.Builder()
                .pingInterval(PING_INTERVAL_SECONDS, TimeUnit.SECONDS)   // notices a dead connection
                .retryOnConnectionFailure(true)
                .build();

        IntentFilter filter = new IntentFilter(LogConfig.ACTION_SETTINGS_CHANGED);
        filter.addAction(LogConfig.ACTION_FORWARDER_STATE_QUERY);
        filter.addAction(LogConfig.ACTION_PERF_LOG_ACCESS_REQUEST);
        ContextCompat.registerReceiver(this, mReceiver, filter, ContextCompat.RECEIVER_NOT_EXPORTED);

        mRunning = true;
        mSenderThread = new Thread(this::senderLoop, "LogForwarderSender");
        mSenderThread.setDaemon(true);
        mSenderThread.start();

        Log.i(TAG, "Log forwarder started in process " + Process.myPid() + "; server " + mServerUrl
                + "; product name (ro.product.name) " + mProduct);
        publishState(true);
        connect();
        applyPerfLog(settings.perfLog);
    }

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        return mBinder;
    }

    @Override
    public void onDestroy() {
        // The service's final batch is delivered on a Binder thread and can still be on its way:
        // give it a moment, then give the buffer a moment to reach the server.
        awaitDrained();

        mRunning = false;
        try {
            unregisterReceiver(mReceiver);
        } catch (IllegalArgumentException ignored) {
            // Never registered.
        }
        mScheduler.shutdownNow();
        if (mPerfTail != null) {
            mPerfTail.stop();
        }

        WebSocket open;
        synchronized (mConnLock) {
            mGeneration++;   // ignore the callbacks the close below causes
            open = mWebSocket;
            mWebSocket = null;
            mLatest = null;
            mConnLock.notifyAll();
        }
        if (open != null) {
            open.close(1000, "Service stopped");   // sends what is already queued first
        }
        if (mSenderThread != null) {
            mSenderThread.interrupt();
            try {
                mSenderThread.join(SENDER_JOIN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        mHttpClient.dispatcher().executorService().shutdown();
        mHttpClient.connectionPool().evictAll();

        publishState(false);
        Log.i(TAG, "Log forwarder stopped; " + mEvents.size() + " buffered event(s) were not delivered");
        super.onDestroy();
    }

    /* ================= Sending ================= */

    /** Takes events from the buffer, formats them and writes them to the open WebSocket. */
    private void senderLoop() {
        try {
            while (mRunning) {
                try {
                    WebSocket ws;
                    synchronized (mConnLock) {
                        while (mRunning && mWebSocket == null) {
                            mConnLock.wait(CONNECTION_WAIT_MILLIS);   // not connected: events stay buffered
                        }
                        ws = mWebSocket;
                    }
                    if (ws == null) {
                        continue;
                    }
                    if (ws.queueSize() > MAX_SOCKET_QUEUE_BYTES) {
                        Thread.sleep(SOCKET_BUSY_PAUSE_MILLIS);   // the network is the slow part: let events buffer here
                        continue;
                    }
                    sendPendingStatus(ws);
                    mPerfOutbox.drain(System.nanoTime(), PERF_MESSAGES_PER_TURN, ws::send);
                    long dropped = mDropped.getAndSet(0);
                    if (dropped > 0) {
                        send(ws, noticeLine("Dropped " + dropped + " buffered log event(s): the server was too slow "
                                + "or unreachable for too long"));
                    }
                    LogEvent event = mEvents.pollFirst(IDLE_POLL_MILLIS, TimeUnit.MILLISECONDS);
                    if (event == null) {
                        continue;
                    }
                    if (!send(ws, toLines(event))) {
                        // The connection is going down; its callback clears mWebSocket. Keep the event.
                        mEvents.offerFirst(event);
                        Thread.sleep(SOCKET_BUSY_PAUSE_MILLIS);
                    }
                } catch (RuntimeException e) {
                    // A bug here must not end log forwarding until the process is restarted.
                    Log.e(TAG, "Unexpected error in the sender; continuing", e);
                    Thread.sleep(SOCKET_BUSY_PAUSE_MILLIS);
                }
            }
        } catch (InterruptedException e) {
            // onDestroy()
        }
    }

    /** Sends the newest device status if it hasn't gone out yet. */
    private void sendPendingStatus(WebSocket ws) {
        if (mStatusPending.compareAndSet(true, false)) {
            String json = mLatestStatus;
            if (json != null && !ws.send(json)) {
                mStatusPending.set(true);   // the connection is going down: send it on the next one
            }
        }
    }

    /* ================= The demo application's performance log ================= */

    /**
     * Switches the reading of the demo application's log on or off, as the setting says: on the main thread, when the
     * forwarder starts and when the screen changes the setting. Switching it on starts the reader at once -- and the
     * screen is open then, so Android can ask the user to allow it (see LogAccess).
     */
    private void applyPerfLog(boolean enabled) {
        if (enabled == mPerfLogEnabled && (mPerfTail != null) == enabled) {
            return;   // nothing changed (the setting broadcast also carries the other settings)
        }
        mPerfLogEnabled = enabled;
        mLogAccess.setEnabled(enabled);
        if (enabled) {
            startPerformanceTail();
        } else {
            stopPerformanceTail();
            Log.i(TAG, "The performance log is switched off: the demo application's log is not read");
        }
        publishState(true);
    }

    /**
     * The screen asks, because the user tapped it: start the reader again right now, while the app is on screen --
     * the only moment Android asks the user to allow it (a run that was started at another moment was declined without
     * a question, and a run that was let in has no need of this).
     */
    private void requestPerfLogAccess() {
        if (!mPerfLogEnabled) {
            Log.i(TAG, "Log access was asked for, but the performance log is switched off");
            return;
        }
        long waitMillis = mLogAccess.askAgainInMillis(System.nanoTime());
        if (waitMillis > 0) {
            // Android would answer from what it remembers of the last request, or decline this one on a stale timer.
            Log.i(TAG, "Log access was asked for too soon after the last reader was started; not before " + (waitMillis / 1000 + 1) + " s");
            publishState(true);
            return;
        }
        Log.i(TAG, "Asking Android to let this app read the demo application's log: the logcat reader is started again");
        mLogAccess.setPermitted(checkSelfPermission(Manifest.permission.READ_LOGS) == PackageManager.PERMISSION_GRANTED);
        if (mPerfTail == null || !mPerfTail.restartNow()) {
            stopPerformanceTail();
            startPerformanceTail();
        }
        publishState(true);
    }

    /**
     * Starts following logcat for the "PerformanceMetrics" tag: each line is parsed (see PerformanceMetric) and
     * queued for the server. The lines are another application's, so without READ_LOGS -- which only adb can
     * grant a normal app -- Android does not even ask the user, and logcat shows the reader only this app's own
     * lines: it starts all the same (a grant takes effect when the app is next started) and this says what is
     * missing. Logd's own lines are asked for as well: they show the reader was let in (see LogAccess).
     */
    private void startPerformanceTail() {
        boolean canReadLogs = checkSelfPermission(Manifest.permission.READ_LOGS) == PackageManager.PERMISSION_GRANTED;
        mLogAccess.setPermitted(canReadLogs);
        mPerfLineSeen = false;
        mOddLineReported = false;
        Log.i(TAG, "Following the \"" + PerformanceMetric.LOG_TAG + "\" log of the demo application; READ_LOGS "
                + (canReadLogs ? "is granted; Android asks the user to allow the reader, and only while the app is on screen"
                : "is NOT granted, so the log lines of other apps are not visible -- grant it once with: adb shell pm grant "
                + getPackageName() + " " + Manifest.permission.READ_LOGS + " (then start the app again)"));
        mPerfTail = new LogcatTail("LogForwarderLogcat",
                nowMillis -> LogcatTail.command(PerformanceMetric.LOG_TAG, nowMillis, LogAccess.LOGD_FILTER),
                new LogcatTail.Listener() {
                    @Override
                    public void onLine(String line) {
                        onPerformanceLine(line);
                    }

                    @Override
                    public void onNotice(String text) {
                        Log.i(TAG, "logcat for " + PerformanceMetric.LOG_TAG + ": " + text);
                    }

                    @Override
                    public void onRunStarted() {
                        mLogAccess.onRunStarted(System.nanoTime());
                        publishState(true);
                    }

                    @Override
                    public void onRunEnded() {
                        mLogAccess.onRunEnded();
                        publishState(true);
                    }
                }, LogcatTail.Timing.DEFAULT);
        mPerfTail.start();
    }

    /** Ends the reading of the demo application's log. */
    private void stopPerformanceTail() {
        LogcatTail tail = mPerfTail;
        mPerfTail = null;
        if (tail != null) {
            tail.stop();
        }
    }

    /** Logcat reading thread: a line of the log is parsed and queued for the server. */
    private void onPerformanceLine(String line) {
        if (LogAccess.isLogdLine(line)) {
            // Logd's own line about a reader that connected: only a reader that Android let in is given it.
            if (mLogAccess.onProof()) {
                Log.i(TAG, "Android lets this app read other apps' log lines (proof: " + line + ")");
                publishState(true);
            }
            return;
        }
        PerformanceMetric metric = PerformanceMetric.parse(line);
        if (metric == null) {
            // logcat's own banners ("--------- beginning of main") are not lines of the log. Anything else it prints
            // is worth saying once: an explanation of a refusal, or a format of the application's that changed.
            if (!mOddLineReported && !line.startsWith("---------")) {
                mOddLineReported = true;
                Log.w(TAG, "Unexpected line from logcat for " + PerformanceMetric.LOG_TAG + " (more are not reported): " + line);
            }
            return;
        }
        long nowNanos = System.nanoTime();
        if (mLogAccess.onPerfLine(nowNanos)) {
            publishState(true);   // a line of the demo application's is proof too
        }
        if (!mPerfLineSeen) {
            mPerfLineSeen = true;
            Log.i(TAG, "First " + PerformanceMetric.LOG_TAG + " line read from logcat: " + metric);
        }
        mPerfOutbox.add(metric.toJson(), nowNanos);
    }

    /** One logcat-threadtime style line per line of the message ("MM-dd HH:mm:ss.SSS PID TID L Tag: text"). */
    private List<String> toLines(LogEvent event) {
        String prefix = String.format(Locale.US, "%s %5d %5d %c %s: ",
                mTimeFormat.format(new Date(event.timeMillis)), event.pid, event.tid,
                levelLetter(event.level), event.tag);
        String[] parts = event.message.split("\n");
        List<String> lines = new ArrayList<>(Math.max(1, parts.length));
        if (parts.length == 0) {
            lines.add(prefix);
        }
        for (String part : parts) {
            lines.add(prefix + part);
        }
        return lines;
    }

    private List<String> noticeLine(String text) {
        List<String> lines = new ArrayList<>(1);
        lines.add(String.format(Locale.US, "%s %5d %5d W %s: %s",
                mTimeFormat.format(new Date()), Process.myPid(), Process.myTid(), TAG, text));
        return lines;
    }

    /** False if the socket refused any line (it is closing). */
    private boolean send(WebSocket ws, List<String> lines) {
        boolean allSent = true;
        for (String line : lines) {
            JSONObject payload = new JSONObject();
            try {
                payload.put("type", "log");
                payload.put("source", SOURCE_NAME);
                payload.put("line", line);
            } catch (JSONException e) {
                continue;
            }
            allSent &= ws.send(payload.toString());
        }
        return allSent;
    }

    private static char levelLetter(int level) {
        switch (level) {
            case Log.VERBOSE:
                return 'V';
            case Log.DEBUG:
                return 'D';
            case Log.INFO:
                return 'I';
            case Log.WARN:
                return 'W';
            case Log.ERROR:
                return 'E';
            default:
                return '?';
        }
    }

    /* ================= The server connection ================= */

    /** Starts one connection attempt, unless one is open or already under way. */
    private void connect() {
        String url;
        long generation;
        synchronized (mConnLock) {
            if (!mRunning || mWebSocket != null || mAttemptInFlight) {
                return;
            }
            mAttemptInFlight = true;
            url = mServerUrl;
            generation = ++mGeneration;
        }
        Request request;
        try {
            request = new Request.Builder().url(url).build();
        } catch (IllegalArgumentException e) {
            Log.e(TAG, "Not a usable log server address: " + url);
            synchronized (mConnLock) {
                mAttemptInFlight = false;
            }
            return;   // nothing to retry until the address changes
        }
        WebSocket ws = mHttpClient.newWebSocket(request, new Listener(generation));
        synchronized (mConnLock) {
            if (generation == mGeneration) {
                mLatest = ws;
            } else {
                ws.cancel();   // the address changed while this was being created
            }
        }
    }

    /** Switches to a new server address: drops the current connection and connects to the new one. */
    private void changeServerUrl(String url) {
        WebSocket toCancel;
        synchronized (mConnLock) {
            if (url.equals(mServerUrl)) {
                return;
            }
            Log.i(TAG, "Log server address changed: " + mServerUrl + " -> " + url);
            mServerUrl = url;
            mGeneration++;   // callbacks of the old connection are ignored from now on
            toCancel = mWebSocket != null ? mWebSocket : mLatest;
            mWebSocket = null;
            mLatest = null;
            mAttemptInFlight = false;
        }
        if (toCancel != null) {
            toCancel.cancel();
        }
        publishState(true);
        connect();
    }

    private void onConnectionLost(long generation) {
        synchronized (mConnLock) {
            if (generation != mGeneration) {
                return;   // replaced by an address change
            }
            mWebSocket = null;
            mAttemptInFlight = false;
        }
        publishState(true);
        if (mRunning) {
            try {
                mScheduler.schedule(this::connect, RECONNECT_DELAY_SECONDS, TimeUnit.SECONDS);
            } catch (RejectedExecutionException ignored) {
                // Shutting down.
            }
        }
    }

    private final class Listener extends WebSocketListener {
        private final long mGen;

        Listener(long generation) {
            mGen = generation;
        }

        @Override
        public void onOpen(WebSocket webSocket, Response response) {
            String url;
            synchronized (mConnLock) {
                if (mGen != mGeneration || !mRunning) {
                    webSocket.cancel();
                    return;
                }
                mWebSocket = webSocket;
                mAttemptInFlight = false;
                url = mServerUrl;
                if (mLatestStatus != null) {
                    mStatusPending.set(true);   // a new connection starts with the device's current state
                }
                mConnLock.notifyAll();   // wake the sender
            }
            Log.i(TAG, "Connected to log server " + url);
            publishState(true);
        }

        @Override
        public void onClosing(WebSocket webSocket, int code, String reason) {
            webSocket.close(code, reason);   // complete the server's close handshake
        }

        @Override
        public void onClosed(WebSocket webSocket, int code, String reason) {
            Log.i(TAG, "Log server connection closed (" + code + " " + reason + "); retrying in "
                    + RECONNECT_DELAY_SECONDS + " s");
            onConnectionLost(mGen);
        }

        @Override
        public void onFailure(WebSocket webSocket, Throwable t, @Nullable Response response) {
            Log.w(TAG, "Log server connection failed (" + t + "); retrying in " + RECONNECT_DELAY_SECONDS + " s");
            onConnectionLost(mGen);
        }
    }

    /* ================= Status for the UI ================= */

    private void publishState(boolean running) {
        boolean connected;
        String url;
        synchronized (mConnLock) {
            connected = mWebSocket != null;
            url = mServerUrl;
        }
        long nowNanos = System.nanoTime();
        sendBroadcast(new Intent(LogConfig.ACTION_FORWARDER_STATE)
                .setPackage(getPackageName())
                .putExtra(LogConfig.EXTRA_FORWARDER_RUNNING, running)
                .putExtra(LogConfig.EXTRA_SERVER_CONNECTED, connected)
                .putExtra(LogConfig.EXTRA_SERVER_URL, url)
                .putExtra(LogConfig.EXTRA_LOG_ACCESS, mLogAccess.state(nowNanos).name())
                .putExtra(LogConfig.EXTRA_PERF_LINES, mLogAccess.perfLines())
                .putExtra(LogConfig.EXTRA_PERF_LINE_AGE_MILLIS, mLogAccess.lastPerfLineAgeMillis(nowNanos))
                .putExtra(LogConfig.EXTRA_ASK_AGAIN_IN_MILLIS, mLogAccess.askAgainInMillis(nowNanos)));
    }

    /** On stop: waits for the buffer to reach the server, as long as it is connected. */
    private void awaitDrained() {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(STOP_FLUSH_MILLIS);
        long quietNanos = TimeUnit.MILLISECONDS.toNanos(STOP_QUIET_MILLIS);
        try {
            while (System.nanoTime() < deadline) {
                boolean connected;
                synchronized (mConnLock) {
                    connected = mWebSocket != null;
                }
                if (!connected) {
                    return;   // nothing can be delivered now
                }
                if (mEvents.isEmpty() && !mStatusPending.get() && System.nanoTime() - mLastEventNanos > quietNanos) {
                    return;
                }
                Thread.sleep(STOP_POLL_MILLIS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
