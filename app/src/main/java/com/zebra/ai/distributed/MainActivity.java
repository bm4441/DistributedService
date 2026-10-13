package com.zebra.ai.distributed;

import android.Manifest;
import android.annotation.SuppressLint;
import android.app.ActivityManager;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.SystemClock;
import android.util.Log;
import android.view.KeyCharacterMap;
import android.view.KeyEvent;
import android.view.View;
import android.view.inputmethod.EditorInfo;
import android.view.inputmethod.InputMethodManager;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.widget.SwitchCompat;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;
import androidx.core.graphics.Insets;
import androidx.core.view.ViewCompat;
import androidx.core.view.WindowInsetsCompat;

import java.util.List;

public class MainActivity extends AppCompatActivity {

    public static final String ACTION_SERVICE_STATE = "com.zebra.ai.distributed.SERVICE_STATE_CHANGED";
    public static final String EXTRA_IS_RUNNING = "is_running";

    private static final String TAG = "DistributedService";
    // Runtime process name is <applicationId>:<process-suffix> (from android:process=":remote_service"
    // in the manifest). applicationId is now "com.zebra.ai.distributed" -- keep this in sync with it.
    private static final String REMOTE_PROCESS_NAME = "com.zebra.ai.distributed:remote_service";
    // Same for the log forwarder (android:process=":log_forwarder").
    private static final String FORWARDER_PROCESS_NAME = "com.zebra.ai.distributed:log_forwarder";
    private static final int NOTIFICATION_PERMISSION_CODE = 101;
    // While this screen is visible, how often the forwarder's status is re-checked.
    private static final long STATUS_REFRESH_INTERVAL_MILLIS = 1500;
    // After "Turn ON Service" with the performance log on, this screen stays up -- Android asks the user to allow
    // log access only while the app is on screen (see LogAccess) -- until it is allowed, or at most this long.
    private static final long LOG_ACCESS_WAIT_MILLIS = 20_000;
    // Once it is allowed, how long the screen stays so that the user sees it.
    private static final long LEAVE_AFTER_ALLOWED_MILLIS = 1_200;

    private TextView tvStatus;
    private TextView tvForwarderStatus;
    private TextView tvServerStatus;
    private SwitchCompat swPerfLog;
    private TextView tvPerfLogStatus;
    private TextView tvPerfLogHint;
    private Button btnPerfLogAccess;
    private SwitchCompat swLogPrint;
    private Spinner spEventLevel;
    private EditText etServerAddress;
    private Button btnApplyServer;
    private Button btnToggleService;
    private final Handler mHandler = new Handler(Looper.getMainLooper());

    private boolean mCurrentRunningState = false;
    private boolean mForwarderRunning = false;
    // Set when the forwarder announces it stopped, until it announces it started again: its
    // process can linger for a moment after that, and must not be reported as running meanwhile.
    private boolean mForwarderStopAnnounced = false;
    private boolean mServerConnected = false;
    // What the forwarder last said about reading the demo app's performance log (see LogAccess): whether Android lets
    // it see the lines, and how many it has read. Only meaningful while the forwarder runs.
    private LogAccess.State mLogAccess = LogAccess.State.OFF;
    private long mPerfLines = 0;
    private long mPerfLineAgeMillis = -1;
    // When (SystemClock.elapsedRealtime) log access can be asked for again; the button counts down to it (see LogAccess.ASK_AGAIN_NANOS).
    private long mAskAgainAtMillis = 0;
    // Set by "Turn ON Service" while the performance log is on: leave the screen as soon as log access is allowed.
    private boolean mLeaveWhenAllowed = false;
    private final Runnable mLeaveRunnable = this::leaveScreen;
    // The Zebra scanner (DataWedge keystroke output): what it types is told from what a person types (see ScanCapture, handleScannerKey()).
    private final ScanCapture mScan = new ScanCapture();
    private final Runnable mScanFinishRunnable = this::finishScan;
    // The address field as it was when the scan began: put back if the scanned code is not a server address.
    private String mFieldBeforeScan = "";
    private int mSelectionBeforeScan = 0;
    // The saved log settings (see LogConfig) as of the last change made on this screen.
    private LogConfig.Settings mSettings;

    // 🔴 Live BroadcastReceiver that receives exact on/off events from DistributedService, and
    // the log forwarder's own start/stop/server-connection events
    private final BroadcastReceiver mStateReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            String action = intent.getAction();
            if (ACTION_SERVICE_STATE.equals(action)) {
                boolean isRunning = intent.getBooleanExtra(EXTRA_IS_RUNNING, false);
                Log.i(TAG, "Broadcast received: service isRunning=" + isRunning);
                mCurrentRunningState = isRunning;
                updateUIState(isRunning);
            } else if (LogConfig.ACTION_FORWARDER_STATE.equals(action)) {
                boolean running = intent.getBooleanExtra(LogConfig.EXTRA_FORWARDER_RUNNING, false);
                mForwarderStopAnnounced = !running;
                mForwarderRunning = running;
                mServerConnected = running && intent.getBooleanExtra(LogConfig.EXTRA_SERVER_CONNECTED, false);
                if (running && intent.hasExtra(LogConfig.EXTRA_LOG_ACCESS)) {
                    mLogAccess = LogAccess.parse(intent.getStringExtra(LogConfig.EXTRA_LOG_ACCESS));
                    mPerfLines = intent.getLongExtra(LogConfig.EXTRA_PERF_LINES, 0);
                    mPerfLineAgeMillis = intent.getLongExtra(LogConfig.EXTRA_PERF_LINE_AGE_MILLIS, -1);
                    mAskAgainAtMillis = SystemClock.elapsedRealtime() + intent.getLongExtra(LogConfig.EXTRA_ASK_AGAIN_IN_MILLIS, 0);
                } else if (!running) {
                    forgetLogAccess();
                }
                updateForwarderUI();
            }
        }
    };

    // Re-checks the forwarder while the screen is visible, in case it was killed without saying so.
    private final Runnable mRefreshRunnable = new Runnable() {
        @Override
        public void run() {
            refreshForwarderStatus();
            mHandler.postDelayed(this, STATUS_REFRESH_INTERVAL_MILLIS);
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        fitToSystemBars();

        tvStatus = findViewById(R.id.tvStatus);
        tvForwarderStatus = findViewById(R.id.tvForwarderStatus);
        tvServerStatus = findViewById(R.id.tvServerStatus);
        swPerfLog = findViewById(R.id.swPerfLog);
        tvPerfLogStatus = findViewById(R.id.tvPerfLogStatus);
        tvPerfLogHint = findViewById(R.id.tvPerfLogHint);
        btnPerfLogAccess = findViewById(R.id.btnPerfLogAccess);
        swLogPrint = findViewById(R.id.swLogPrint);
        spEventLevel = findViewById(R.id.spEventLevel);
        etServerAddress = findViewById(R.id.etServerAddress);
        btnApplyServer = findViewById(R.id.btnApplyServer);
        btnToggleService = findViewById(R.id.btnToggleService);

        requestNotificationPermission();
        setUpLogSettings();
        updatePerfLogUI();

        btnToggleService.setOnClickListener(v -> {
            if (mCurrentRunningState) {
                stopDistributedService();
            } else {
                startDistributedServiceAndExit();
            }
        });
    }

    /**
     * Keeps this screen clear of the navigation bar and of the keyboard. Since Android 15 an app that targets SDK 35 or
     * higher is drawn edge to edge: its window is neither kept above the navigation bar nor made smaller when the
     * keyboard opens. So the keyboard covered the lower half of the screen, the "Log server address" field with it, and
     * the buttons at the bottom lay under the navigation bar.
     *
     * The room they take up is now given up as padding at the bottom of the root (and at its sides, in landscape),
     * which makes the scroll view shorter. As it shrinks, it scrolls to the field being edited -- what it did when the
     * system still made the window smaller itself. The insets this root is given are what the action bar leaves of the
     * window's (ActionBarOverlayLayout keeps the status bar's share for itself), so the top is not counted twice; and
     * a system that still resizes the window itself gives it none, so nothing is counted twice there either.
     */
    private void fitToSystemBars() {
        View root = findViewById(R.id.screenRoot);
        ViewCompat.setOnApplyWindowInsetsListener(root, (view, windowInsets) -> {
            Insets bars = windowInsets.getInsets(
                    WindowInsetsCompat.Type.systemBars() | WindowInsetsCompat.Type.displayCutout());
            Insets keyboard = windowInsets.getInsets(WindowInsetsCompat.Type.ime());
            // The keyboard's height is measured from the bottom edge of the screen, the navigation bar's room included:
            // the larger of the two is what is covered, not their sum.
            view.setPadding(bars.left, 0, bars.right, Math.max(bars.bottom, keyboard.bottom));
            return WindowInsetsCompat.CONSUMED;
        });
        ViewCompat.requestApplyInsets(root);
    }

    @Override
    protected void onStart() {
        super.onStart();
        // Register receiver for real-time state changes. ContextCompat.registerReceiver()
        // handles the pre/post API 33 branching internally (RECEIVER_NOT_EXPORTED is only
        // meaningful on 33+; on older versions it's ignored), so there's no bare
        // registerReceiver() call left for the "missing export flag" lint check to flag.
        IntentFilter filter = new IntentFilter(ACTION_SERVICE_STATE);
        filter.addAction(LogConfig.ACTION_FORWARDER_STATE);
        ContextCompat.registerReceiver(this, mStateReceiver, filter, ContextCompat.RECEIVER_NOT_EXPORTED);
    }

    @Override
    protected void onResume() {
        super.onResume();
        // Check if remote process exists upon opening
        mCurrentRunningState = isProcessActive(REMOTE_PROCESS_NAME,
                ActivityManager.RunningAppProcessInfo.IMPORTANCE_FOREGROUND_SERVICE);
        updateUIState(mCurrentRunningState);
        mHandler.post(mRefreshRunnable);
    }

    @Override
    protected void onPause() {
        super.onPause();
        mHandler.removeCallbacks(mRefreshRunnable);
        mHandler.removeCallbacks(mScanFinishRunnable);
        mScan.reset();
    }

    @Override
    protected void onStop() {
        super.onStop();
        try {
            unregisterReceiver(mStateReceiver);
        } catch (Exception ignored) {}
    }

    /**
     * True if this app has a process with this name that is actually doing something -- its
     * importance is at or above (numerically at or below) maxImportance -- and not just a dead,
     * cached leftover.
     */
    private boolean isProcessActive(String processName, int maxImportance) {
        ActivityManager am = (ActivityManager) getSystemService(Context.ACTIVITY_SERVICE);
        if (am != null) {
            List<ActivityManager.RunningAppProcessInfo> processes = am.getRunningAppProcesses();
            if (processes != null) {
                for (ActivityManager.RunningAppProcessInfo procInfo : processes) {
                    if (processName.equals(procInfo.processName)) {
                        return procInfo.importance <= maxImportance;
                    }
                }
            }
        }
        return false;
    }

    private void startDistributedServiceAndExit() {
        Intent serviceIntent = new Intent(this, DistributedService.class);
        ContextCompat.startForegroundService(this, serviceIntent);

        mCurrentRunningState = true;
        updateUIState(true);
        mHandler.removeCallbacks(mLeaveRunnable);
        if (mSettings.perfLog) {
            // The log forwarder asks Android to let it read the demo app's log as it starts, and Android asks the user
            // only while this app is on screen: stay until that is answered (the screen leaves by itself once log access
            // is allowed, or after a while whatever the answer), instead of leaving after 300 ms as without it.
            Toast.makeText(this, R.string.toast_service_started_wait, Toast.LENGTH_LONG).show();
            mLeaveWhenAllowed = true;
            mHandler.postDelayed(mLeaveRunnable, LOG_ACCESS_WAIT_MILLIS);
        } else {
            Toast.makeText(this, "DistributedService started in background.", Toast.LENGTH_SHORT).show();
            mHandler.postDelayed(mLeaveRunnable, 300);
        }
    }

    /** Sends this screen to the background and closes it: the service goes on without it. */
    private void leaveScreen() {
        mLeaveWhenAllowed = false;
        moveTaskToBack(true);
        finish();
    }

    private void stopDistributedService() {
        // 🔴 Update UI state to STOPPED immediately
        mCurrentRunningState = false;
        mLeaveWhenAllowed = false;
        mHandler.removeCallbacks(mLeaveRunnable);
        updateUIState(false);

        // A plain stopService() would leave the service running while an application is bound to it -- a requestor is, for as long as
        // its session lasts. The service is told to turn itself off instead: it releases what it holds, lets go of the log forwarder
        // (so both services go off), removes its notification and ends. Without a session it is destroyed at once, as before.
        Intent serviceIntent = new Intent(this, DistributedService.class);
        try {
            startService(serviceIntent.setAction(DistributedService.ACTION_TURN_OFF));
        } catch (IllegalStateException e) {
            Log.w(TAG, "Could not send the turn-off command; stopping the service the plain way", e);
            stopService(new Intent(this, DistributedService.class));
        }

        Toast.makeText(this, "DistributedService stopped.", Toast.LENGTH_SHORT).show();
    }

    @SuppressLint("SetTextI18n")
    private void updateUIState(boolean isRunning) {
        if (isRunning) {
            tvStatus.setText("Service Status: RUNNING");
            tvStatus.setTextColor(Color.parseColor("#2E7D32")); // Green
            btnToggleService.setText("Turn OFF Service");
            btnToggleService.setBackgroundColor(Color.parseColor("#C62828")); // Red
        } else {
            tvStatus.setText("Service Status: STOPPED");
            tvStatus.setTextColor(Color.parseColor("#D32F2F")); // Red
            btnToggleService.setText("Turn ON Service");
            btnToggleService.setBackgroundColor(Color.parseColor("#2E7D32")); // Green
        }
    }

    /** Re-checks the forwarder's process and, if it's up, asks it for its server connection state. */
    private void refreshForwarderStatus() {
        mForwarderRunning = !mForwarderStopAnnounced && isProcessActive(FORWARDER_PROCESS_NAME,
                ActivityManager.RunningAppProcessInfo.IMPORTANCE_CACHED - 1);
        if (mForwarderRunning) {
            // Answered with ACTION_FORWARDER_STATE, which mStateReceiver handles.
            sendBroadcast(new Intent(LogConfig.ACTION_FORWARDER_STATE_QUERY).setPackage(getPackageName()));
        } else {
            mServerConnected = false;
            forgetLogAccess();
        }
        updateForwarderUI();
    }

    /** The forwarder is not running: what it said about log access is not true any more. */
    private void forgetLogAccess() {
        mLogAccess = LogAccess.State.OFF;
        mPerfLines = 0;
        mPerfLineAgeMillis = -1;
        mAskAgainAtMillis = 0;
    }

    @SuppressLint("SetTextI18n")
    private void updateForwarderUI() {
        if (mForwarderRunning) {
            tvForwarderStatus.setText("Log Forwarder Status: RUNNING");
            tvForwarderStatus.setTextColor(Color.parseColor("#2E7D32")); // Green
        } else {
            tvForwarderStatus.setText("Log Forwarder Status: STOPPED");
            tvForwarderStatus.setTextColor(Color.parseColor("#D32F2F")); // Red
        }
        if (mForwarderRunning && mServerConnected) {
            tvServerStatus.setText("Log Server: CONNECTED");
            tvServerStatus.setTextColor(Color.parseColor("#2E7D32")); // Green
        } else {
            tvServerStatus.setText("Log Server: DISCONNECTED");
            tvServerStatus.setTextColor(Color.parseColor("#D32F2F")); // Red
        }
        updatePerfLogUI();
    }

    /* ================= Performance log: the demo app's PerformanceMetrics lines (see LogAccess) ================= */

    /** True if this app holds READ_LOGS (only adb can grant it): without it Android does not even ask the user. */
    private boolean hasReadLogs() {
        return ContextCompat.checkSelfPermission(this, Manifest.permission.READ_LOGS) == PackageManager.PERMISSION_GRANTED;
    }

    /**
     * The tap on "Ask Android to allow log access": the forwarder starts its logcat reader again right now, while this
     * screen is open, which is when Android asks the user "Allow one-time access?".
     */
    private void askForLogAccess() {
        mLogAccess = LogAccess.State.STARTING;   // until the forwarder says what became of it
        mAskAgainAtMillis = SystemClock.elapsedRealtime() + LogAccess.ASK_AGAIN_NANOS / 1_000_000L;   // the forwarder says the exact time
        updatePerfLogUI();
        sendBroadcast(new Intent(LogConfig.ACTION_PERF_LOG_ACCESS_REQUEST).setPackage(getPackageName()));
    }

    /** "3 s", "2 min": how long ago the demo app's last line was read. */
    private static String ageText(long millis) {
        if (millis < 1000) {
            return "<1 s";
        }
        return millis < 60_000 ? (millis / 1000) + " s" : (millis / 60_000) + " min";
    }

    /** The performance log's switch, its line, what to do next, and the button -- from the setting and what the forwarder said. */
    private void updatePerfLogUI() {
        boolean on = mSettings.perfLog;
        boolean canAsk = false;
        int status;
        String color;
        String hint;
        if (!on) {
            status = R.string.perf_log_status_off;
            color = "#757575"; // Grey
            hint = getString(R.string.perf_log_hint_off);
        } else if (!hasReadLogs()) {
            status = R.string.perf_log_status_no_permission;
            color = "#EF6C00"; // Amber
            hint = getString(R.string.perf_log_hint_no_permission, getPackageName());
        } else if (!mForwarderRunning) {
            status = R.string.perf_log_status_not_running;
            color = "#757575"; // Grey
            hint = getString(R.string.perf_log_hint_not_running);
        } else {
            // Nothing reported yet, or just switched on: Android is being asked.
            LogAccess.State state = (mLogAccess == LogAccess.State.OFF || mLogAccess == LogAccess.State.NEEDS_PERMISSION)
                    ? LogAccess.State.STARTING : mLogAccess;
            switch (state) {
                case GRANTED:
                    status = R.string.perf_log_status_granted;
                    color = "#2E7D32"; // Green
                    hint = (mPerfLines > 0
                            ? getString(R.string.perf_log_lines, mPerfLines, ageText(mPerfLineAgeMillis))
                            : getString(R.string.perf_log_lines_none))
                            + "\n" + getString(R.string.perf_log_hint_granted);
                    break;
                case WAITING:
                    status = R.string.perf_log_status_waiting;
                    color = "#D32F2F"; // Red
                    hint = getString(R.string.perf_log_hint_waiting);
                    canAsk = true;
                    break;
                default:
                    status = R.string.perf_log_status_starting;
                    color = "#EF6C00"; // Amber
                    hint = getString(R.string.perf_log_hint_starting);
                    canAsk = true;
                    break;
            }
            if (state == LogAccess.State.GRANTED && mLeaveWhenAllowed) {
                // "Turn ON Service" is waiting for this: leave once the user has seen that it is allowed.
                mLeaveWhenAllowed = false;
                mHandler.removeCallbacks(mLeaveRunnable);
                mHandler.postDelayed(mLeaveRunnable, LEAVE_AFTER_ALLOWED_MILLIS);
            }
        }
        // Android keeps its answer for a minute and a stale timer of its own can decline a request made soon after the last
        // one (see LogAccess.ASK_AGAIN_NANOS): the button waits, and says for how long.
        long waitMillis = mAskAgainAtMillis - SystemClock.elapsedRealtime();
        boolean ready = waitMillis <= 0;
        tvPerfLogStatus.setText(status);
        tvPerfLogStatus.setTextColor(Color.parseColor(color));
        tvPerfLogHint.setText(hint);
        btnPerfLogAccess.setEnabled(canAsk && ready);
        btnPerfLogAccess.setText(canAsk && !ready
                ? getString(R.string.perf_log_ask_again, (waitMillis + 999) / 1000)
                : getString(R.string.perf_log_ask));
    }

    /* ================= Log settings: Log Print, Event Level, server address ================= */

    private void setUpLogSettings() {
        mSettings = LogConfig.read(this);

        // Each control is given its saved value before its listener is attached, so setting it
        // doesn't count as the user changing it.
        swLogPrint.setChecked(mSettings.logPrint);
        swLogPrint.setOnCheckedChangeListener((button, checked) ->
                saveSettings(checked, mSettings.eventLevel, mSettings.serverUrl, mSettings.perfLog));

        // Switching the performance log on while the forwarder runs starts its logcat reader at once -- this screen is
        // open, so Android can ask the user to allow it.
        swPerfLog.setChecked(mSettings.perfLog);
        swPerfLog.setOnCheckedChangeListener((button, checked) -> {
            saveSettings(mSettings.logPrint, mSettings.eventLevel, mSettings.serverUrl, checked);
            if (!checked) {
                // "Turn ON Service" was waiting for log access: nobody is asking for it any more, and the user is here.
                mLeaveWhenAllowed = false;
                mHandler.removeCallbacks(mLeaveRunnable);
            }
            updatePerfLogUI();
        });
        btnPerfLogAccess.setOnClickListener(v -> askForLogAccess());

        ArrayAdapter<String> levels = new ArrayAdapter<>(this,
                android.R.layout.simple_spinner_item, LogConfig.LEVEL_LABELS);
        levels.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        spEventLevel.setAdapter(levels);
        spEventLevel.setSelection(LogConfig.levelToIndex(mSettings.eventLevel), false);
        spEventLevel.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
            @Override
            public void onItemSelected(AdapterView<?> parent, View view, int position, long id) {
                int level = LogConfig.LEVELS[position];
                // Also fires once when the screen is first laid out; nothing to save then.
                if (level != mSettings.eventLevel) {
                    saveSettings(mSettings.logPrint, level, mSettings.serverUrl, mSettings.perfLog);
                }
            }

            @Override
            public void onNothingSelected(AdapterView<?> parent) {
            }
        });

        etServerAddress.setText(mSettings.serverUrl);
        btnApplyServer.setOnClickListener(v -> applyServerAddress());
        etServerAddress.setOnEditorActionListener((v, actionId, event) -> {
            if (actionId == EditorInfo.IME_ACTION_DONE) {
                applyServerAddress();
                return true;
            }
            return false;
        });
    }

    /** Saves the settings and tells the service and forwarder processes if they're running. */
    private void saveSettings(boolean logPrint, int eventLevel, String serverUrl, boolean perfLog) {
        mSettings = new LogConfig.Settings(logPrint, eventLevel, serverUrl, perfLog);
        LogConfig.write(this, mSettings);
        LogConfig.broadcast(this, mSettings);
    }

    private void applyServerAddress() {
        String address = LogConfig.normalizeServerUrl(etServerAddress.getText().toString());
        if (address == null) {
            etServerAddress.setError("Enter an address like ws://192.168.1.10:5000/logs");
            return;
        }
        etServerAddress.setError(null);
        etServerAddress.setText(address);   // show it the way it's saved
        etServerAddress.clearFocus();
        InputMethodManager keyboard = getSystemService(InputMethodManager.class);
        if (keyboard != null) {
            keyboard.hideSoftInputFromWindow(etServerAddress.getWindowToken(), 0);
        }
        saveSettings(mSettings.logPrint, mSettings.eventLevel, address, mSettings.perfLog);
        Toast.makeText(this, "Log server address applied.", Toast.LENGTH_SHORT).show();
    }

    /* ================= Zebra scanner: scan the dashboard's QR code to set the address and turn the service on ================= */

    /**
     * Sees every key event the screen gets (the Zebra scanner's DataWedge profile types what it scans as key events, into the field
     * that has the focus -- or into nothing, if none does) and picks out a scan: a run of characters that comes in a few
     * milliseconds, where a person takes a tenth of a second per key (see ScanCapture). A person's typing, the soft keyboard, the Apply
     * button and the Done key are not touched: only what is taken for a scan is handled here, and only its Enter is swallowed.
     */
    @Override
    public boolean dispatchKeyEvent(KeyEvent event) {
        return handleScannerKey(event) || super.dispatchKeyEvent(event);
    }

    /** True if the event belongs to a scan and is not to go on to the field or a button. */
    private boolean handleScannerKey(KeyEvent event) {
        long now = event.getEventTime();
        int code = event.getKeyCode();
        int action = event.getAction();
        if (action == KeyEvent.ACTION_MULTIPLE && code == KeyEvent.KEYCODE_UNKNOWN && event.getCharacters() != null
                && event.getCharacters().length() >= ScanCapture.MIN_CHARS) {
            // The whole text in one event: nothing is typed into the field, the scan is read from here.
            if (mScan.onChunk(event.getCharacters(), now)) {
                rememberFieldBeforeScan();
            }
            scheduleScanFinish();
            return true;
        }
        if (code == KeyEvent.KEYCODE_ENTER || code == KeyEvent.KEYCODE_NUMPAD_ENTER) {
            // The Enter a scanner can add after the code: it must not press the button that has the focus, or apply a half-finished field.
            return mScan.takesEnter(now);
        }
        if (action == KeyEvent.ACTION_DOWN && event.getRepeatCount() == 0
                && !event.isCtrlPressed() && !event.isAltPressed() && !event.isMetaPressed()) {
            int unicode = event.getUnicodeChar();
            if (unicode >= 0x20 && unicode != 0x7F && unicode <= 0xFFFF && (unicode & KeyCharacterMap.COMBINING_ACCENT) == 0) {
                if (mScan.onChar((char) unicode, now)) {
                    rememberFieldBeforeScan();           // (before this character reaches the field)
                }
                scheduleScanFinish();
                // The key goes on to the field as it always did; if it turns out to be part of a scan, the field is set (or put back) below.
            }
        }
        return false;
    }

    private void rememberFieldBeforeScan() {
        mFieldBeforeScan = etServerAddress.getText().toString();
        mSelectionBeforeScan = etServerAddress.getSelectionStart();
    }

    private void scheduleScanFinish() {
        mHandler.removeCallbacks(mScanFinishRunnable);
        mHandler.postDelayed(mScanFinishRunnable, ScanCapture.SETTLE_MILLIS + 25);
    }

    /**
     * The scanner has gone quiet. If what it typed was a scan:
     *   - a log server address -> it replaces what the field had (the old address, and whatever of the scan was typed into it), is
     *     applied the way the Apply button does, and the service is turned on (as "Turn ON Service" does) unless it is on already;
     *   - anything else -> the field is put back as it was, with an error: the old address is kept.
     * If it was a person typing, nothing is done.
     */
    private void finishScan() {
        String scanned = mScan.finish(SystemClock.uptimeMillis());
        if (scanned == null) {
            if (!mScan.isEmpty()) {
                scheduleScanFinish();                    // not over yet: look again
            }
            return;
        }
        String address = ScannedAddress.parse(scanned);
        if (address == null) {
            etServerAddress.setText(mFieldBeforeScan);
            etServerAddress.setSelection(Math.max(0, Math.min(mSelectionBeforeScan, mFieldBeforeScan.length())));
            String message = "The scanned code is not a log server address. Scan the dashboard's QR code (ws://<PC address>:5000/logs).";
            etServerAddress.setError(message);
            Toast.makeText(this, message, Toast.LENGTH_LONG).show();
            Log.w(TAG, "Scanned a code that is not a log server address (" + scanned.length() + " characters): the address is unchanged");
            return;
        }
        Log.i(TAG, "Scanned the log server address " + address);
        etServerAddress.setText(address);
        etServerAddress.setError(null);
        applyServerAddress();
        if (!mCurrentRunningState) {
            startDistributedServiceAndExit();
        }
    }

    private void requestNotificationPermission() {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS)
                != PackageManager.PERMISSION_GRANTED) {
            ActivityCompat.requestPermissions(
                    this,
                    new String[]{Manifest.permission.POST_NOTIFICATIONS},
                    NOTIFICATION_PERMISSION_CODE
            );
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == NOTIFICATION_PERMISSION_CODE) {
            if (grantResults.length > 0 && grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                Toast.makeText(this, "Notification permission granted", Toast.LENGTH_SHORT).show();
            }
        }
    }
}
