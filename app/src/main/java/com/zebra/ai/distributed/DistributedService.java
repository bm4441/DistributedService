package com.zebra.ai.distributed;

import static com.zebra.ai.distributed.MainActivity.ACTION_SERVICE_STATE;
import static com.zebra.ai.distributed.MainActivity.EXTRA_IS_RUNNING;

import android.annotation.SuppressLint;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.os.Build;
import android.os.IBinder;
import android.provider.Settings;

import androidx.core.app.NotificationCompat;

import com.zebra.ai.vision.detector.AIVisionSDK;
import com.zebra.ai.vision.internal.distributed.LenderHandler;


import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Foreground service tying together the five engines that used to all live in this one file
 * (kept as separate classes purely for size/organization, per request -- no functionality or
 * implementation changed in the split):
 *  - MulticastDiscoveryEngine: multicast peer discovery (join/leave, MSG/ACK wire protocol,
 *    per-request bookkeeping, IPeerDiscoveryListener registrations).
 *  - GrpcTransportEngine: gRPC server lifecycle, per-peer channel pool, backpressure, and the
 *    single gRPC service impl that dispatches every incoming stream to whichever engine below
 *    actually handles that kind of payload.
 *  - PeerReadinessEngine: the ready-check/ready-ack/release-notice protocol and per-capability
 *    LenderHandler warm-up/release lifecycle.
 *  - TaskTransferEngine: submitting/sending distinct tasks and receiving/processing them.
 *  - ResultTransferEngine: sending a processed task's result back and receiving/dispatching
 *    results for this device's own submitted tasks.
 * All still reach into this class's shared state (Wi-Fi network, executors, known-peers map,
 * local serial number) via a plain back-reference, exactly as when it was all one class.
 */
public class DistributedService extends Service {

    public static final String PREFS_NAME = "DistributedServicePrefs";
    public static final String KEY_IS_RUNNING = "is_distributed_service_running";

    private static final String TAG = "DistributedService";
    private static final String GREETING = "Hello, I am a new system service";

    private static final String CHANNEL_ID = "DistributedServiceChannel";
    private static final int NOTIFICATION_ID = 2001;

    // The global setting that holds this device's serial number -- see getLocalSerialNumber().
    private static final String SERIAL_SETTING_KEY = "distributed_service_serial";

    // How often the "device" message (the device's name and stats) goes to the dashboard -- see sendDeviceStatus().
    private static final long DEVICE_MESSAGE_PERIOD_SECONDS = 1;

    private final NetworkInterfaceServiceImpl mNetworkInterfaceBinder = new NetworkInterfaceServiceImpl(this);
    private volatile String mLocalSerialNumber;

    // How much of the time the models are being warmed up or run -- the DSP/NPU usage the dashboard shows (see BusyMeter).
    final BusyMeter mInferenceMeter = new BusyMeter();

    // The conditions the device must meet for the service to run (battery, temperature) and the latest verdict on them. Judged
    // on every device reading, on the scheduler thread (the only one that touches the two fields after it); nothing acts on it yet.
    private final ServicePolicy mServicePolicy = new ServicePolicy();
    private volatile ServicePolicy.Decision mPolicyDecision;
    private String mLastPolicyKey;
    private boolean mFirstPolicyReading = true;

    // A Contributor until an application binds to the service, a Requestor while one is bound -- see onBind(). Written on the
    // main thread, where the service's lifecycle callbacks run, and read from any thread.
    private volatile ServiceMode mServiceMode = ServiceMode.CONTRIBUTOR;
    // The bindings that are open. Android calls onBind() for the first client of an intent and onUnbind() when the last
    // client of that intent is gone, so those two (and onRebind()) are what is counted. Main thread only.
    private int mOpenBindings;

    /* ================= Wi-Fi Callbacks ================= */
    volatile Network mWifiNetwork;
    ConnectivityManager mConnectivityManager;

    private final ConnectivityManager.NetworkCallback mWifiNetworkCallback =
            new ConnectivityManager.NetworkCallback() {
                @Override
                public void onAvailable(@NonNull Network network) {
                    EventLog.i(TAG, "Wi-Fi connected on network " + network);
                    mWifiNetwork = network;
                    mDiscoveryEngine.startMulticastListener(network);
                    mTransferExecutor.execute(mGrpcEngine::startTransferServer);
                }

                @Override
                public void onLost(@NonNull Network network) {
                    EventLog.i(TAG, "Wi-Fi disconnected from network " + network);
                    mWifiNetwork = null;
                    mKnownDevices.clear();
                    mDiscoveryEngine.mPendingRequests.clear();
                    mDiscoveryEngine.stopMulticastListener();
                    mGrpcEngine.stopTransferServer();
                }
            };

    /* ================= Threading & Shared State ================= */
    final ScheduledExecutorService mScheduler = Executors.newScheduledThreadPool(2);
    final ExecutorService mOutboundExecutor = Executors.newFixedThreadPool(4);
    final ExecutorService mTransferExecutor = Executors.newCachedThreadPool();

    final ConcurrentHashMap<String, PeerInfo> mKnownDevices = new ConcurrentHashMap<>();

    /* ================= Engines ================= */
    final MulticastDiscoveryEngine mDiscoveryEngine = new MulticastDiscoveryEngine(this);
    final GrpcTransportEngine mGrpcEngine = new GrpcTransportEngine(this);
    final PeerReadinessEngine mReadinessEngine = new PeerReadinessEngine(this);
    final TaskTransferEngine mTaskEngine = new TaskTransferEngine(this);
    final ResultTransferEngine mResultEngine = new ResultTransferEngine(this);

    // Installed once per process: onCreate() runs again if the service is restarted in the same
    // process, and a second copy would log every crash twice. Only touched on the main thread.
    private static boolean sCrashLoggerInstalled;

    @Override
    public void onCreate() {
        super.onCreate();
        // First, so everything below -- and the crash logger -- already logs through it. This also
        // starts the log-forwarder process (see EventLog.init()).
        EventLog.init(this);
        installCrashLogger();
        createNotificationChannel();
        // A new service has no application bound yet, so it starts as a Contributor (an application that started it by
        // binding is reported by onBind(), right after this).
        logServiceMode("the service started; no application is bound");
        // From the start, so the dashboard knows this device's name at once (starting the SDK below can take a while).
        mScheduler.scheduleWithFixedDelay(this::sendDeviceStatus, 0, DEVICE_MESSAGE_PERIOD_SECONDS, TimeUnit.SECONDS);
        initializeLenderHandlers();
    }

    /**
     * Logs any uncaught exception, on any thread of this process, under this service's own log tag
     * before handing it to the system's handler, which then crashes the process exactly as it would
     * have anyway. The system's own crash report goes to the AndroidRuntime tag, which a
     * "logcat -s DistributedService" filter hides. Native crashes (inside the SDK's .so libraries)
     * aren't Java exceptions and don't come through here -- those need "logcat -b crash".
     * Goes through EventLog.fatal(): always written to logcat, whatever the Log Print setting,
     * and the events still queued are handed to the forwarder before the process dies.
     */
    private static void installCrashLogger() {
        if (sCrashLoggerInstalled) {
            return;
        }
        sCrashLoggerInstalled = true;
        Thread.UncaughtExceptionHandler systemHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            EventLog.fatal(TAG, "FATAL: uncaught exception on thread " + thread.getName()
                    + " -- the process is about to crash", throwable);
            if (systemHandler != null) {
                systemHandler.uncaughtException(thread, throwable);
            }
        });
    }

    /**
     * Initializes the AI Vision SDK -- required before any LenderHandler/model construction.
     * This is the fix for the crash seen without it: prepare() failed deep inside
     * TextOCR.Settings's constructor with "AIVisionSDKException: AI Vision SDK is not
     * initialized", because nothing had ever called this.

     * Deliberately does NOT eagerly build any LenderHandlers here: DistributedLending.
     * createHandlers() built every known capability up front, and product.frame's
     * ModuleRecognizer.Settings resolves its model file at construction time (unlike
     * ocr.recognize, which defers all asset access into prepare()) -- so calling it required
     * bundling a 378MB model archive into this project's own assets/, which isn't wanted.
     * Handlers are instead built lazily, per capability, on first use -- see
     * PeerReadinessEngine.getOrCreateLenderHandler().
     */
    private void initializeLenderHandlers() {
        try {
            boolean initialized = AIVisionSDK.getInstance(this).init();
            EventLog.i(TAG, "AIVisionSDK.init() " + (initialized ? "succeeded" : "returned false"));
        } catch (Exception e) {
            EventLog.e(TAG, "AIVisionSDK.init() failed", e);
        }
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        setServiceRunningState(this, true);

        Notification notification = buildNotification();
        startForeground(NOTIFICATION_ID, notification);
        broadcastState(true);

        if (!mDiscoveryEngine.isListening() && mConnectivityManager == null) {
            EventLog.i(TAG, GREETING + " [onStartCommand - Initializing]");

            mConnectivityManager = getSystemService(ConnectivityManager.class);
            if (mConnectivityManager != null) {
                try {
                    NetworkRequest wifiRequest = new NetworkRequest.Builder()
                            .addTransportType(NetworkCapabilities.TRANSPORT_WIFI)
                            .removeCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                            .build();
                    mConnectivityManager.registerNetworkCallback(wifiRequest, mWifiNetworkCallback);
                } catch (Exception e) {
                    EventLog.e(TAG, "Failed to register network callback", e);
                }
            }
        } else {
            EventLog.i(TAG, "DistributedService already active, ignoring redundant start.");
        }

        return START_STICKY;
    }

    /** An application bound to the service: it is a Requestor from now on. */
    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        applicationBound();
        return mNetworkInterfaceBinder;
    }

    /**
     * The last application that bound with this intent is gone. Returns true so that onRebind() is called when an
     * application binds again: Android calls onBind() only the first time.
     */
    @Override
    public boolean onUnbind(Intent intent) {
        applicationUnbound();
        return true;
    }

    /** An application binds again, after the last one had unbound (see onUnbind()). */
    @Override
    public void onRebind(Intent intent) {
        super.onRebind(intent);
        applicationBound();
    }

    /** The service's mode: a Requestor while an application is bound to it, a Contributor otherwise. */
    ServiceMode getServiceMode() {
        return mServiceMode;
    }

    private void applicationBound() {
        mOpenBindings++;
        if (mServiceMode != ServiceMode.REQUESTOR) {
            mServiceMode = ServiceMode.REQUESTOR;
            logServiceMode("an application bound to the service");
        }
    }

    private void applicationUnbound() {
        if (mOpenBindings > 0) {
            mOpenBindings--;
        }
        if (mOpenBindings == 0 && mServiceMode != ServiceMode.CONTRIBUTOR) {
            mServiceMode = ServiceMode.CONTRIBUTOR;
            logServiceMode("the last application unbound from the service");
        }
    }

    /** Info, so that it also reaches the dashboard server at the default Event Level. */
    private void logServiceMode(String why) {
        EventLog.i(TAG, "The Service Mode: " + mServiceMode.label + " (" + why + ")");
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        broadcastState(false);
        setServiceRunningState(this, false);

        if (mConnectivityManager != null) {
            try {
                mConnectivityManager.unregisterNetworkCallback(mWifiNetworkCallback);
            } catch (Exception ignored) {}
            mConnectivityManager = null;
        }

        mDiscoveryEngine.stopMulticastListener();
        mGrpcEngine.stopTransferServer();

        mScheduler.shutdownNow();
        mOutboundExecutor.shutdownNow();
        mTransferExecutor.shutdownNow();
        mTaskEngine.shutdownPeerWorkers(0);
        mTaskEngine.shutdownProcessingThread();
        mResultEngine.shutdownResultTransferThread();

        // Same as PeerReadinessEngine.releaseCapability(): removed first, then disposed under the
        // handler's lock, so an in-progress handle()/prepare() finishes before its model is freed.
        for (String capability : mReadinessEngine.mLenderHandlers.keySet()) {
            LenderHandler handler = mReadinessEngine.mLenderHandlers.remove(capability);
            if (handler != null) {
                synchronized (handler) {
                    try {
                        handler.dispose();
                    } catch (Exception e) {
                        EventLog.w(TAG, "Error disposing LenderHandler", e);
                    }
                }
            }
        }

        EventLog.i(TAG, "DistributedService destroyed and stopped.");
        // Last: delivers the final events to the forwarder, then unbinds it so it flushes to the
        // server and stops.
        EventLog.shutdown();
    }

    private void broadcastState(boolean isRunning) {
        Intent intent = new Intent(ACTION_SERVICE_STATE);
        intent.setPackage(getPackageName());
        intent.putExtra(EXTRA_IS_RUNNING, isRunning);
        sendBroadcast(intent);
    }

    private Notification buildNotification() {
        Intent notificationIntent = new Intent(this, MainActivity.class);
        notificationIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);

        PendingIntent pendingIntent = PendingIntent.getActivity(
                this,
                0,
                notificationIntent,
                PendingIntent.FLAG_IMMUTABLE | PendingIntent.FLAG_UPDATE_CURRENT
        );

        return new NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle("Distributed Service Active")
                .setContentText("Listening for peer discovery & file transfers.")
                .setSmallIcon(android.R.drawable.ic_dialog_info)
                .setContentIntent(pendingIntent)
                .setOngoing(true)
                .setPriority(NotificationCompat.PRIORITY_LOW)
                .build();
    }

    private void createNotificationChannel() {
        NotificationChannel serviceChannel = new NotificationChannel(
                CHANNEL_ID,
                "Distributed Service Channel",
                NotificationManager.IMPORTANCE_LOW
        );
        NotificationManager manager = getSystemService(NotificationManager.class);
        if (manager != null) {
            manager.createNotificationChannel(serviceChannel);
        }
    }

    /**
     * This device's id: its serial number -- the system property ro.serialno, e.g. "252695242D0034" -- which an app cannot
     * read itself. It is put once on each device, from a PC, in the global setting distributed_service_serial:
     *     adb shell 'settings put global distributed_service_serial $(getprop ro.serialno)'
     * and the service reads it from there when it starts, so after running it start the service again. A device without
     * that setting keeps the id it had before: the Android ID, and for an emulator a name made from the model. Read once;
     * the line "Local serial number: <id>" is matched by the dashboard server, keep its form.
     */
    @SuppressLint("HardwareIds")
    String getLocalSerialNumber() {
        if (mLocalSerialNumber == null) {
            synchronized (this) {
                if (mLocalSerialNumber == null) {
                    String serial = readSerialNumberSetting();
                    if (serial != null) {
                        mLocalSerialNumber = serial;
                    } else {
                        // Uses Android 64-bit unique device ID (safe for non-system APKs)
                        String deviceId = Settings.Secure.getString(
                                getContentResolver(),
                                Settings.Secure.ANDROID_ID
                        );

                        if (deviceId != null && !deviceId.trim().isEmpty() && !"null".equalsIgnoreCase(deviceId)) {
                            mLocalSerialNumber = deviceId;
                        } else {
                            // Fallback in case of emulator or unique testing environment
                            mLocalSerialNumber = "DEV_" + Build.MODEL.replaceAll("[^a-zA-Z0-9]", "_");
                        }
                    }
                    EventLog.i(TAG, "Local serial number: " + mLocalSerialNumber);
                    if (serial == null) {
                        EventLog.w(TAG, "This device's id is not its serial number. To use it, run once from a PC: adb shell 'settings put global "
                                + SERIAL_SETTING_KEY + " $(getprop ro.serialno)' and start the service again");
                    }
                }
            }
        }
        return mLocalSerialNumber;
    }

    /**
     * The serial number in the global setting distributed_service_serial, or null if there is none to use: the setting is not
     * there, or holds something that cannot be an id. The ids travel in "|"-separated messages, so only letters, digits, "."
     * "_" and "-" are accepted (a serial number is made of that), and not a placeholder such as "unknown".
     */
    private String readSerialNumberSetting() {
        String value = Settings.Global.getString(getContentResolver(), SERIAL_SETTING_KEY);
        if (value == null) {
            return null;
        }
        value = value.trim();
        if (!value.matches("[A-Za-z0-9._-]{1,64}") || "unknown".equalsIgnoreCase(value) || "null".equalsIgnoreCase(value)) {
            EventLog.w(TAG, "Ignoring the setting " + SERIAL_SETTING_KEY + ": '" + value + "' is not a serial number");
            return null;
        }
        return value;
    }

    /**
     * Tells the dashboard what this device is and how it is doing: a "device" message with its id and model and its
     * stats -- the battery percentage, the available memory and the DSP/NPU usage (see DeviceStats). The log forwarder
     * process adds the product name (ro.product.name, "TC501W") as the message passes through -- the dashboard calls the
     * device by that name, "TC501W-1" when several share it, and picks its icon by it. The dashboard expects a status
     * every few seconds, so this runs on the scheduler from the start of the service. The forwarder keeps only the newest
     * and sends it again whenever its connection to the server is (re)established, so a server that is started later gets
     * it too.
     */
    private void sendDeviceStatus() {
        try {
            DeviceStats stats = DeviceStats.read(this, mInferenceMeter);
            ServicePolicy.Decision decision = mServicePolicy.evaluate(stats);
            mPolicyDecision = decision;
            logPolicyIfChanged(decision);
            EventLog.sendDeviceStatus(stats.toJson(getLocalSerialNumber(), Build.MODEL, System.currentTimeMillis(), decision));
        } catch (RuntimeException e) {
            // A task on the scheduler that throws is never run again: say so, and try again at the next period.
            EventLog.e(TAG, "Could not send the device's status to the dashboard", e);
        }
    }

    /** The service policy's latest verdict (see ServicePolicy); null until the first reading, a moment after the service starts. */
    ServicePolicy.Decision getServicePolicyDecision() {
        return mPolicyDecision;
    }

    /**
     * Logs the verdict the first time and whenever a rule's verdict changes -- not with every reading. Info, so that it
     * also reaches the dashboard at the default Event Level. For now the policy is only reported: nothing acts on it yet.
     */
    private void logPolicyIfChanged(ServicePolicy.Decision decision) {
        if (mFirstPolicyReading) {
            mFirstPolicyReading = false;
            EventLog.i(TAG, "Service policy: " + mServicePolicy.describe());
        }
        if (decision.key.equals(mLastPolicyKey)) {
            return;
        }
        mLastPolicyKey = decision.key;
        EventLog.i(TAG, "Service policy " + (decision.met ? "met" : "NOT met") + ": " + decision.summary);
    }

    /* =========================================================================
     * Entry points for NetworkInterfaceServiceImpl -- thin delegation to the engines above.
     * ========================================================================= */

    void requestPeerDiscovery(String capability, IPeerDiscoveryListener listener) {
        mDiscoveryEngine.requestPeerDiscovery(capability, listener);
    }

    void stopPeerDiscovery(String capability) {
        mReadinessEngine.stopPeerDiscovery(capability);
    }

    List<PeerInfo> getKnownPeers() {
        return mDiscoveryEngine.getKnownPeers();
    }

    void registerDiscoveryListener(IPeerDiscoveryListener listener) {
        mDiscoveryEngine.registerDiscoveryListener(listener);
    }

    void unregisterDiscoveryListener(IPeerDiscoveryListener listener) {
        mDiscoveryEngine.unregisterDiscoveryListener(listener);
    }

    void getReadyPeers(List<PeerInfo> peers, String capability) {
        mReadinessEngine.getReadyPeers(peers, capability);
    }

    void submitDistinctTasks(List<DistinctTaskEnvelope> distinctTaskList, ITaskResponseCallback callback) {
        mTaskEngine.submitDistinctTasks(distinctTaskList, callback);
    }

    void cancelTaskGroup(String rootGroupId) {
        mResultEngine.cancelTaskGroup(rootGroupId);
    }

    public static void setServiceRunningState(Context context, boolean isRunning) {
        SharedPreferences prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE);
        prefs.edit().putBoolean(KEY_IS_RUNNING, isRunning).apply();
    }

}
