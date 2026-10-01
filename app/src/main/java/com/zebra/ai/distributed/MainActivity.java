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
import android.util.Log;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

import java.util.List;

public class MainActivity extends AppCompatActivity {

    public static final String ACTION_SERVICE_STATE = "com.zebra.ai.distributed.SERVICE_STATE_CHANGED";
    public static final String EXTRA_IS_RUNNING = "is_running";

    private static final String TAG = "DistributedService";
    // Runtime process name is <applicationId>:<process-suffix> (from android:process=":remote_service"
    // in the manifest). applicationId is now "com.zebra.ai.distributed" -- keep this in sync with it.
    private static final String REMOTE_PROCESS_NAME = "com.zebra.ai.distributed:remote_service";
    private static final int NOTIFICATION_PERMISSION_CODE = 101;

    private TextView tvStatus;
    private Button btnToggleService;
    private final Handler mHandler = new Handler(Looper.getMainLooper());

    private boolean mCurrentRunningState = false;

    // 🔴 Live BroadcastReceiver that receives exact on/off events from DistributedService
    private final BroadcastReceiver mStateReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            if (ACTION_SERVICE_STATE.equals(intent.getAction())) {
                boolean isRunning = intent.getBooleanExtra(EXTRA_IS_RUNNING, false);
                Log.i(TAG, "Broadcast received: service isRunning=" + isRunning);
                mCurrentRunningState = isRunning;
                updateUIState(isRunning);
            }
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        tvStatus = findViewById(R.id.tvStatus);
        btnToggleService = findViewById(R.id.btnToggleService);

        requestNotificationPermission();

        btnToggleService.setOnClickListener(v -> {
            if (mCurrentRunningState) {
                stopDistributedService();
            } else {
                startDistributedServiceAndExit();
            }
        });
    }

    @Override
    protected void onStart() {
        super.onStart();
        // Register receiver for real-time state changes. ContextCompat.registerReceiver()
        // handles the pre/post API 33 branching internally (RECEIVER_NOT_EXPORTED is only
        // meaningful on 33+; on older versions it's ignored), so there's no bare
        // registerReceiver() call left for the "missing export flag" lint check to flag.
        IntentFilter filter = new IntentFilter(ACTION_SERVICE_STATE);
        ContextCompat.registerReceiver(this, mStateReceiver, filter, ContextCompat.RECEIVER_NOT_EXPORTED);
    }

    @Override
    protected void onResume() {
        super.onResume();
        // Check if remote process exists upon opening
        mCurrentRunningState = isRemoteProcessActive();
        updateUIState(mCurrentRunningState);
    }

    @Override
    protected void onStop() {
        super.onStop();
        try {
            unregisterReceiver(mStateReceiver);
        } catch (Exception ignored) {}
    }

    private boolean isRemoteProcessActive() {
        ActivityManager am = (ActivityManager) getSystemService(Context.ACTIVITY_SERVICE);
        if (am != null) {
            List<ActivityManager.RunningAppProcessInfo> processes = am.getRunningAppProcesses();
            if (processes != null) {
                for (ActivityManager.RunningAppProcessInfo procInfo : processes) {
                    if (REMOTE_PROCESS_NAME.equals(procInfo.processName)) {
                        // Importance <= FOREGROUND_SERVICE means the service is actively running, not just a dead cached process
                        return procInfo.importance <= ActivityManager.RunningAppProcessInfo.IMPORTANCE_FOREGROUND_SERVICE;
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
        Toast.makeText(this, "DistributedService started in background.", Toast.LENGTH_SHORT).show();

        mHandler.postDelayed(() -> {
            moveTaskToBack(true);
            finish();
        }, 300);
    }

    private void stopDistributedService() {
        // 🔴 Update UI state to STOPPED immediately
        mCurrentRunningState = false;
        updateUIState(false);

        Intent serviceIntent = new Intent(this, DistributedService.class);
        stopService(serviceIntent);

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
