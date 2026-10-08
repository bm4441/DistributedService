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

    private final NetworkInterfaceServiceImpl mNetworkInterfaceBinder = new NetworkInterfaceServiceImpl(this);
    private volatile String mLocalSerialNumber;

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

    @Nullable
    @Override
    public IBinder onBind(Intent intent) {
        return mNetworkInterfaceBinder;
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

    @SuppressLint("HardwareIds")
    String getLocalSerialNumber() {
        if (mLocalSerialNumber == null) {
            synchronized (this) {
                if (mLocalSerialNumber == null) {
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
                    EventLog.i(TAG, "Local serial number: " + mLocalSerialNumber);
                }
            }
        }
        return mLocalSerialNumber;
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
