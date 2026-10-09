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
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.SharedPreferences;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.os.Build;
import android.os.IBinder;
import android.os.PowerManager;
import android.os.SystemClock;
import android.provider.Settings;

import androidx.core.app.NotificationCompat;
import androidx.core.content.ContextCompat;

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

    // The service runs only while its policy is met. "Running" means the engines -- the discovery sockets, the gRPC server
    // and connections, the processing and result threads, the models; the service itself stays up (its notification, the
    // log forwarder, the status sent to the dashboard) so that it can notice the policy being met again.
    // The gate decides (on the scheduler thread) when a verdict has held long enough to act on; the engines are started or
    // stopped on mTransferExecutor, one at a time (mActivationLock), towards what the gate last decided (mWantActive).
    private final ServicePolicy.Gate mPolicyGate =
            new ServicePolicy.Gate(ServicePolicy.START_AFTER_MILLIS, ServicePolicy.STOP_AFTER_MILLIS);
    private volatile boolean mServiceActive;
    private volatile boolean mWantActive;
    private volatile long mActiveSinceMillis = System.currentTimeMillis();     // when mServiceActive last changed
    private final Object mActivationLock = new Object();
    private PowerManager.WakeLock mPolicyWakeLock;                              // under mPolicyCheckLock
    private volatile boolean mDestroyed;

    // The policy is checked every second (sendDeviceStatus()) and, besides, the moment the battery changes -- a plug, a level, a
    // temperature -- (onBatteryChanged()): a device that is asleep, with its screen off, runs its one-second timer only while it
    // happens to be awake, but the system delivers the battery broadcast when it wakes for the change. Both checks go through
    // mPolicyCheckLock, which also serializes the gate. Wake locks keep the CPU awake for what must not be left half done
    // (see acquireWorkLock()): the engines starting or stopping, the wind-down of a Contributor, a battery event being handled.
    private static final long WORK_WAKE_LOCK_MILLIS = 90_000;              // the longest a wind-down can take, with its notices
    private static final long BATTERY_EVENT_WAKE_LOCK_MILLIS = 3_000;      // from the broadcast to the check on the scheduler
    private final Object mPolicyCheckLock = new Object();
    private final java.util.concurrent.atomic.AtomicBoolean mPolicyCheckQueued = new java.util.concurrent.atomic.AtomicBoolean();
    private BroadcastReceiver mBatteryReceiver;

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
                    // Whoever this device was reserved for is out of reach now, and its release notice cannot arrive.
                    mReadinessEngine.releaseReservation();
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
        registerBatteryReceiver();
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
            applyServiceModeToDiscovery();
            leaveContributorRole();
        }
    }

    private void applicationUnbound() {
        if (mOpenBindings > 0) {
            mOpenBindings--;
        }
        if (mOpenBindings == 0 && mServiceMode != ServiceMode.CONTRIBUTOR) {
            mServiceMode = ServiceMode.CONTRIBUTOR;
            logServiceMode("the last application unbound from the service");
            applyServiceModeToDiscovery();
        }
    }

    /**
     * The service became a Requestor: whatever it was doing as a Contributor is wound down -- the requestor that holds it is
     * told it is withdrawn, the tasks it already accepted are completed and their results sent, and its processing and
     * result-transfer threads are released (see PeerReadinessEngine.leaveContributorRole()). It waits for tasks, so not on
     * the main thread, where the bind callbacks run.
     */
    private void leaveContributorRole() {
        // The lock is taken here, while the CPU is surely awake, and released when the wind-down is over: the device may be
        // asleep otherwise before the executor's thread gets to run, and be frozen half-way through finishing the tasks.
        PowerManager.WakeLock lock = acquireWorkLock("LeaveContributor", WORK_WAKE_LOCK_MILLIS);
        try {
            mTransferExecutor.execute(() -> {
                try {
                    mReadinessEngine.leaveContributorRole();
                } finally {
                    releaseWorkLock(lock);
                }
            });
        } catch (RuntimeException e) {
            releaseWorkLock(lock);
            // The executor is shut down: the service is being destroyed, and everything is released there.
        }
    }

    /**
     * The mode changed: a Requestor stops listening for discovery requests (and so cannot answer them) and keeps only what
     * it needs to send its own; a Contributor listens again. Network work, so not on the main thread, where the bind
     * callbacks run. It reads the mode when it runs, so quick changes one after the other end in the right state.
     */
    private void applyServiceModeToDiscovery() {
        try {
            mTransferExecutor.execute(mDiscoveryEngine::applyServiceMode);
        } catch (RuntimeException e) {
            // The executor is shut down: the service is being destroyed, and everything is closed.
        }
    }

    /** Info, so that it also reaches the dashboard server at the default Event Level. */
    private void logServiceMode(String why) {
        EventLog.i(TAG, "The Service Mode: " + mServiceMode.label + " (" + why + ")");
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        mDestroyed = true;
        if (mBatteryReceiver != null) {
            try {
                unregisterReceiver(mBatteryReceiver);
            } catch (IllegalArgumentException ignored) {
                // Never registered.
            }
            mBatteryReceiver = null;
        }
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

        ServicePolicy.Decision decision = mPolicyDecision;
        boolean paused = !mServiceActive && decision != null && !decision.met;
        return new NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle(paused ? "Distributed Service paused" : "Distributed Service Active")
                .setContentText(paused ? decision.summary : "Listening for peer discovery & file transfers.")
                .setSmallIcon(android.R.drawable.ic_dialog_info)
                .setContentIntent(pendingIntent)
                .setOngoing(true)
                .setPriority(NotificationCompat.PRIORITY_LOW)
                .build();
    }

    /** Shows the service's state in its notification -- paused, with the reason, or active. */
    private void updateNotification() {
        if (mDestroyed) {
            return;                                    // the notification goes with the service; do not bring it back
        }
        NotificationManager manager = getSystemService(NotificationManager.class);
        if (manager != null) {
            manager.notify(NOTIFICATION_ID, buildNotification());
        }
    }

    /** Registers for battery changes: see onBatteryChanged(). A sticky system broadcast; its first delivery is a check too. */
    private void registerBatteryReceiver() {
        mBatteryReceiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                onBatteryChanged();
            }
        };
        ContextCompat.registerReceiver(this, mBatteryReceiver, new IntentFilter(Intent.ACTION_BATTERY_CHANGED),
                ContextCompat.RECEIVER_NOT_EXPORTED);
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
            DeviceStats stats;
            ServicePolicy.Decision decision;
            synchronized (mPolicyCheckLock) {
                stats = DeviceStats.read(this, mInferenceMeter, true);
                decision = evaluatePolicy(stats);
            }
            // ACTIVE: the engines run. STARTING: the policy is met and the engines are about to start. PAUSED: they are not running.
            String state = mServiceActive ? "ACTIVE" : decision.met ? "STARTING" : "PAUSED";
            EventLog.sendDeviceStatus(stats.toJson(getLocalSerialNumber(), Build.MODEL, System.currentTimeMillis(), decision,
                    state, mActiveSinceMillis));
        } catch (RuntimeException e) {
            // A task on the scheduler that throws is never run again: say so, and try again at the next period.
            EventLog.e(TAG, "Could not send the device's status to the dashboard", e);
        }
    }

    /** True while the service's engines run: its policy is met (see driveServiceByPolicy()). */
    boolean isServiceActive() {
        return mServiceActive;
    }

    /**
     * Feeds the policy's verdict to the gate and starts or stops the engines when the gate says so. The first verdict acts at
     * once; after that a change needs to hold for ServicePolicy.START_AFTER_MILLIS (to start) or STOP_AFTER_MILLIS (to stop),
     * so a reading that hovers at a limit does not start and stop the models over and over. Called with mPolicyCheckLock held
     * (the gate is not thread-safe).
     */
    private void driveServiceByPolicy(ServicePolicy.Decision decision) {
        long now = SystemClock.elapsedRealtime();
        int action = mPolicyGate.update(decision.met, now);
        if (action != ServicePolicy.Gate.NONE) {
            mWantActive = action == ServicePolicy.Gate.START;
            applyPolicyState();
        }
        // While a change is pending the CPU is kept awake for it: a device lying on a charger with the screen off would
        // otherwise go back to sleep right after the battery changed, and start the service only at its next wake-up.
        long untilChange = mPolicyGate.millisUntilChange(now);
        if (untilChange >= 0) {
            if (mPolicyWakeLock == null) {
                PowerManager powerManager = getSystemService(PowerManager.class);
                if (powerManager != null) {
                    mPolicyWakeLock = powerManager.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, TAG + ":PolicyGate");
                    mPolicyWakeLock.setReferenceCounted(false);
                }
            }
            if (mPolicyWakeLock != null) {
                mPolicyWakeLock.acquire(untilChange + 2_000);      // times out by itself
            }
        }
    }

    /** Brings the engines in line with what the gate last decided; idempotent, so quick changes end in the right state. */
    private void applyPolicyState() {
        // Taken here, on the thread that saw the verdict and so is awake, and released when the engines have started or
        // stopped: a device with its screen off may otherwise fall asleep before the executor's thread runs, or half-way
        // through finishing the accepted tasks, and the requestor would be kept waiting.
        PowerManager.WakeLock lock = acquireWorkLock("PolicyApply", WORK_WAKE_LOCK_MILLIS);
        try {
            mTransferExecutor.execute(() -> {
                try {
                    synchronized (mActivationLock) {
                        boolean want = mWantActive;
                        if (mDestroyed || want == mServiceActive) {
                            return;
                        }
                        if (want) {
                            startEngines();
                        } else {
                            pauseEngines();
                        }
                    }
                } finally {
                    releaseWorkLock(lock);
                }
            });
        } catch (RuntimeException e) {
            releaseWorkLock(lock);
            // The executor is shut down: the service is being destroyed.
        }
    }

    /**
     * Judges one reading against the policy, records the verdict (and logs it when a rule's verdict changed) and feeds it to
     * the gate. Called with mPolicyCheckLock held, from the regular status reading and from the battery event.
     */
    private ServicePolicy.Decision evaluatePolicy(DeviceStats stats) {
        ServicePolicy.Decision decision = mServicePolicy.evaluate(stats);
        mPolicyDecision = decision;
        logPolicyIfChanged(decision);
        driveServiceByPolicy(decision);
        return decision;
    }

    /**
     * The battery changed (a charger plugged or unplugged, the level, the temperature): the policy is checked now rather than
     * at the next tick of the one-second timer, which a sleeping device does not run until it wakes up. The check runs on
     * the scheduler; a wake lock keeps the CPU awake from this broadcast to the check, and the gate takes over from there
     * (driveServiceByPolicy() holds the CPU while a change is pending). Several changes close together are one check.
     */
    private void onBatteryChanged() {
        if (mDestroyed || !mPolicyCheckQueued.compareAndSet(false, true)) {
            return;
        }
        PowerManager.WakeLock lock = acquireWorkLock("BatteryEvent", BATTERY_EVENT_WAKE_LOCK_MILLIS);
        try {
            mScheduler.execute(() -> {
                try {
                    mPolicyCheckQueued.set(false);
                    checkPolicyNow();
                } finally {
                    releaseWorkLock(lock);
                }
            });
        } catch (RuntimeException e) {
            mPolicyCheckQueued.set(false);
            releaseWorkLock(lock);
            // The scheduler is shut down: the service is being destroyed.
        }
    }

    /** One policy check, without the regular status message (and without touching the NPU sampling window). */
    private void checkPolicyNow() {
        try {
            synchronized (mPolicyCheckLock) {
                evaluatePolicy(DeviceStats.read(this, mInferenceMeter, false));
            }
        } catch (RuntimeException e) {
            EventLog.e(TAG, "The policy check on a battery change failed", e);
        }
    }

    /** A partial wake lock for work that must not be frozen half-way; it ends by itself after timeoutMillis. Null if none. */
    private PowerManager.WakeLock acquireWorkLock(String name, long timeoutMillis) {
        PowerManager powerManager = getSystemService(PowerManager.class);
        if (powerManager == null) {
            return null;
        }
        PowerManager.WakeLock lock = powerManager.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, TAG + ":" + name);
        lock.setReferenceCounted(false);
        lock.acquire(timeoutMillis);
        return lock;
    }

    private static void releaseWorkLock(PowerManager.WakeLock lock) {
        if (lock != null && lock.isHeld()) {
            lock.release();
        }
    }

    /** The policy is met: the engines start -- on the Wi-Fi network if there is one, otherwise when it comes. */
    private void startEngines() {
        mServiceActive = true;
        mActiveSinceMillis = System.currentTimeMillis();
        EventLog.i(TAG, "Service policy met: the service starts");
        updateNotification();
        Network network = mWifiNetwork;
        if (network == null) {
            EventLog.i(TAG, "Wi-Fi is not connected: the engines open when it is");
            return;
        }
        mDiscoveryEngine.startMulticastListener(network);        // a listener or a request socket, by the service's mode
        mGrpcEngine.startTransferServer();
    }

    /**
     * The policy is not met: the engines stop. In this order: nothing new is accepted (isServiceActive() is false, and the
     * discovery sockets are closed so that nobody can discover this device); the requestor that holds it is told, and the
     * tasks it had accepted are completed and their results sent; what this device was waiting for as a requestor is
     * ended and its callers told; then the gRPC server and the connections close.
     */
    private void pauseEngines() {
        mServiceActive = false;
        mActiveSinceMillis = System.currentTimeMillis();
        ServicePolicy.Decision decision = mPolicyDecision;
        EventLog.w(TAG, "Service policy NOT met: the service pauses" + (decision != null ? " (" + decision.summary + ")" : ""));
        updateNotification();

        mDiscoveryEngine.stopMulticastListener();
        mDiscoveryEngine.failPendingRequests(MulticastDiscoveryEngine.ERROR_SERVICE_PAUSED, "The service is paused by its policy");
        mReadinessEngine.pauseContributorWork();                 // as a Contributor: finish what was accepted, tell its requestor
        mTaskEngine.shutdownPeerWorkers(2000);                   // as a Requestor: uploads stop ...
        mReadinessEngine.releaseHeldPeersAndForget();            // ... the contributors it holds are released ...
        mResultEngine.cancelAllTaskGroups();                     // ... and the application is told its tasks will not complete
        mGrpcEngine.stopTransferServer();
        mGrpcEngine.closeAllChannels();
        mKnownDevices.clear();
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
        // First: from now on getConnectedPeers() does not start this discovery again.
        mDiscoveryEngine.clearLastDiscovery(capability);
        mReadinessEngine.stopPeerDiscovery(capability);
    }

    /** Starts a new peer discovery if the last one is older than the peer discovery age -- see MulticastDiscoveryEngine. */
    void refreshPeersIfExpired() {
        mDiscoveryEngine.refreshPeersIfExpired();
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
