package com.zebra.ai.distributed;

import android.app.ActivityManager;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.BatteryManager;

/**
 * One reading of the device's stats -- the battery percentage, the available memory and the DSP/NPU usage -- and the
 * "device" message that carries it to the dashboard server (see toJson()). A value that could not be read is left at its
 * "unknown" default (-1, or NaN for decimals) and is sent as null, never guessed.
 */
final class DeviceStats {

    /** Marks a message as a device status rather than a log line, for the dashboard server. */
    static final String MESSAGE_TYPE = "device";
    static final String SOURCE_NAME = "DistributedService";

    // Battery (from the sticky ACTION_BATTERY_CHANGED broadcast)
    boolean batteryPresent = true;
    int batteryPercent = -1;                           // 0-100
    boolean charging;
    int plugged;                                       // BatteryManager.BATTERY_PLUGGED_* bits; 0 = on battery
    float batteryTempC = Float.NaN;                    // the battery's temperature, the device's best reading of how warm it is

    // Memory (ActivityManager.getMemoryInfo)
    long memAvailableBytes = -1;
    long memTotalBytes = -1;
    boolean memLow;

    // AI accelerator (DSP/NPU): this service's inference duty cycle -- see BusyMeter
    double npuBusyPercent = Double.NaN;                // NaN for the first reading: there is no window to measure yet
    long npuTasksProcessed;
    long npuLastTaskMillis = -1;

    /**
     * Reads the stats now. The NPU usage is the share of time since the previous call during which the models were busy
     * (BusyMeter.sampleBusyPercent()), so call this from one place only, at regular intervals.
     */
    static DeviceStats read(Context context, BusyMeter inferenceMeter) {
        DeviceStats stats = new DeviceStats();

        // The sticky battery broadcast: reading it needs no receiver, and is always current.
        Intent battery = context.registerReceiver(null, new IntentFilter(Intent.ACTION_BATTERY_CHANGED));
        if (battery != null) {
            stats.batteryPresent = battery.getBooleanExtra(BatteryManager.EXTRA_PRESENT, true);
            int level = battery.getIntExtra(BatteryManager.EXTRA_LEVEL, -1);
            int scale = battery.getIntExtra(BatteryManager.EXTRA_SCALE, -1);
            if (level >= 0 && scale > 0) {
                stats.batteryPercent = (int) (level * 100L / scale);   // rounded down: never more than there is
            }
            int tenthsOfDegree = battery.getIntExtra(BatteryManager.EXTRA_TEMPERATURE, Integer.MIN_VALUE);
            if (tenthsOfDegree != Integer.MIN_VALUE) {
                stats.batteryTempC = tenthsOfDegree / 10f;
            }
            int status = battery.getIntExtra(BatteryManager.EXTRA_STATUS, BatteryManager.BATTERY_STATUS_UNKNOWN);
            stats.plugged = battery.getIntExtra(BatteryManager.EXTRA_PLUGGED, 0);
            stats.charging = status == BatteryManager.BATTERY_STATUS_CHARGING
                    || (status == BatteryManager.BATTERY_STATUS_FULL && stats.plugged != 0);
        }

        ActivityManager activityManager = context.getSystemService(ActivityManager.class);
        if (activityManager != null) {
            ActivityManager.MemoryInfo info = new ActivityManager.MemoryInfo();
            activityManager.getMemoryInfo(info);
            stats.memAvailableBytes = info.availMem;
            stats.memTotalBytes = info.totalMem;
            stats.memLow = info.lowMemory;
        }

        // Android gives an app no reading of the real DSP/NPU load; this service is the only thing on the device that
        // drives the models, so the share of time they were busy running them (see BusyMeter) is the figure it offers.
        stats.npuBusyPercent = inferenceMeter.sampleBusyPercent(System.nanoTime());
        stats.npuTasksProcessed = inferenceMeter.completedCount();
        stats.npuLastTaskMillis = inferenceMeter.lastDurationMillis();
        return stats;
    }

    /**
     * The message the log forwarder sends to the dashboard server for this reading, next to the log lines on the same
     * WebSocket: {"type":"device","source":"DistributedService","ts":...,"deviceId":"252695242D0034","model":"TC501",
     * "battery":{...},"memory":{...},"npu":{...},"policy":{...}} -- the members the dashboard reads (see fleet.py,
     * _clean_status). The forwarder adds the product name ("TC501W") as it passes the message on (see ProductName).
     */
    String toJson(String deviceId, String model, long timeMillis, ServicePolicy.Decision policy) {
        JsonText json = new JsonText().beginObject();
        json.key("type").str(MESSAGE_TYPE);
        json.key("source").str(SOURCE_NAME);
        json.key("ts").num(timeMillis);
        json.key("deviceId").str(deviceId);
        json.key("model").str(model);

        json.key("battery").beginObject();
        json.key("present").bool(batteryPresent);
        if (batteryPercent >= 0) {
            json.key("percent").num(batteryPercent);
        } else {
            json.key("percent").nul();
        }
        json.key("charging").bool(charging);
        json.key("plugged").str(pluggedName(plugged));
        json.key("temperatureC").num(batteryTempC, 1);
        json.endObject();

        json.key("memory").beginObject();
        if (memAvailableBytes >= 0) {
            json.key("availableMb").num(memAvailableBytes / (1024 * 1024));
        } else {
            json.key("availableMb").nul();
        }
        if (memTotalBytes >= 0) {
            json.key("totalMb").num(memTotalBytes / (1024 * 1024));
        } else {
            json.key("totalMb").nul();
        }
        if (memAvailableBytes >= 0 && memTotalBytes > 0) {
            json.key("usedPercent").num(100.0 * (memTotalBytes - memAvailableBytes) / memTotalBytes, 1);
        } else {
            json.key("usedPercent").nul();
        }
        json.key("low").bool(memLow);
        json.endObject();

        json.key("npu").beginObject();
        json.key("busyPercent").num(npuBusyPercent, 1);
        json.key("tasksProcessed").num(npuTasksProcessed);
        if (npuLastTaskMillis >= 0) {
            json.key("lastTaskMs").num(npuLastTaskMillis);
        } else {
            json.key("lastTaskMs").nul();
        }
        json.endObject();

        json.key("policy").beginObject();
        json.key("met").bool(policy.met);
        json.key("summary").str(policy.summary);
        json.key("rules").beginArray();
        for (ServicePolicy.RuleResult rule : policy.rules) {
            json.beginObject();
            json.key("id").str(rule.id);
            json.key("name").str(rule.name);
            json.key("ok").bool(rule.ok);
            json.key("value").str(rule.value);
            json.key("required").str(rule.required);
            json.key("detail").str(rule.detail);
            json.endObject();
        }
        json.endArray();
        json.endObject();

        return json.endObject().toString();
    }

    /** What is powering the device, in the words the dashboard shows: "none" on battery, else "ac", "usb+wireless"... */
    static String pluggedName(int plugged) {
        if (plugged == 0) {
            return "none";
        }
        StringBuilder names = new StringBuilder();
        if ((plugged & BatteryManager.BATTERY_PLUGGED_AC) != 0) {
            names.append("ac");
        }
        if ((plugged & BatteryManager.BATTERY_PLUGGED_USB) != 0) {
            names.append(names.length() > 0 ? "+usb" : "usb");
        }
        if ((plugged & BatteryManager.BATTERY_PLUGGED_WIRELESS) != 0) {
            names.append(names.length() > 0 ? "+wireless" : "wireless");
        }
        if ((plugged & BatteryManager.BATTERY_PLUGGED_DOCK) != 0) {
            names.append(names.length() > 0 ? "+dock" : "dock");
        }
        return names.length() > 0 ? names.toString() : "other";
    }
}
