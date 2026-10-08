package com.zebra.ai.distributed;

import android.content.Context;
import android.content.Intent;
import android.util.AtomicFile;
import android.util.Log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Settings and broadcast contract for the log-forwarding feature, shared by the app's three
 * processes: the UI (MainActivity, main process), the service (":remote_service") and the log
 * forwarder (":log_forwarder").
 *
 * Saved values live in a small file in the app's files dir that every process reads fresh when
 * it starts -- SharedPreferences would not do, since each process caches its own copy and would
 * never see a change another process made. A change is pushed to the processes that are already
 * running as an in-app broadcast (ACTION_SETTINGS_CHANGED).
 */
final class LogConfig {

    private static final String TAG = "LogConfig";
    private static final String SETTINGS_FILE_NAME = "log_settings.properties";
    private static final String KEY_LOG_PRINT = "log_print";
    private static final String KEY_EVENT_LEVEL = "event_level";
    private static final String KEY_SERVER_URL = "server_url";
    private static final String KEY_PERF_LOG = "perf_log";
    /** "host" or "host:port", for names java.net.URI won't accept as a host (see normalizeServerUrl()). */
    private static final Pattern PLAIN_AUTHORITY = Pattern.compile("([A-Za-z0-9._-]+)(?::(\\d{1,5}))?");

    /* ================= Broadcasts (all restricted to this app's own package) ================= */
    /** UI -> service and forwarder: the settings changed; carries all of the values. */
    static final String ACTION_SETTINGS_CHANGED = "com.zebra.ai.distributed.LOG_SETTINGS_CHANGED";
    /** Forwarder -> UI: the forwarder started/stopped, its server connection or its log access changed. */
    static final String ACTION_FORWARDER_STATE = "com.zebra.ai.distributed.LOG_FORWARDER_STATE";
    /** UI -> forwarder: please send ACTION_FORWARDER_STATE now (no reply if it isn't running). */
    static final String ACTION_FORWARDER_STATE_QUERY = "com.zebra.ai.distributed.LOG_FORWARDER_STATE_QUERY";
    /**
     * UI -> forwarder: ask Android to let the forwarder read the demo application's log lines now -- it starts its
     * logcat reader again, at once. Android asks the user only while the app is on screen, so this is sent from the
     * screen, by a tap (see LogAccess).
     */
    static final String ACTION_PERF_LOG_ACCESS_REQUEST = "com.zebra.ai.distributed.PERF_LOG_ACCESS_REQUEST";

    static final String EXTRA_LOG_PRINT = "log_print";
    static final String EXTRA_EVENT_LEVEL = "event_level";
    static final String EXTRA_SERVER_URL = "server_url";
    static final String EXTRA_PERF_LOG = "perf_log";
    static final String EXTRA_FORWARDER_RUNNING = "forwarder_running";
    static final String EXTRA_SERVER_CONNECTED = "server_connected";
    /** In ACTION_FORWARDER_STATE: the name of a LogAccess.State -- whether Android lets the forwarder read the demo's log. */
    static final String EXTRA_LOG_ACCESS = "log_access";
    /** In ACTION_FORWARDER_STATE: how many lines of the demo's log have been read, and (ms) how long ago the last one was; -1 if none. */
    static final String EXTRA_PERF_LINES = "perf_lines";
    static final String EXTRA_PERF_LINE_AGE_MILLIS = "perf_line_age_ms";
    /** In ACTION_FORWARDER_STATE: how long (ms) until log access can be asked for again; 0 if it can be now (see LogAccess.ASK_AGAIN_NANOS). */
    static final String EXTRA_ASK_AGAIN_IN_MILLIS = "ask_again_in_ms";

    /* ================= Defaults ================= */
    /** Write every log line to logcat too, exactly as the service always did. */
    static final boolean DEFAULT_LOG_PRINT = true;
    static final int DEFAULT_EVENT_LEVEL = Log.INFO;
    static final String DEFAULT_SERVER_URL = "ws://172.20.10.7:5000/logs";
    /** Read the demo application's performance log: off, since Android asks the user for it each time (see LogAccess) -- it is for the device that runs the demo. */
    static final boolean DEFAULT_PERF_LOG = false;

    /**
     * Event level meaning "send every event to the server". Its own value, below Verbose (the
     * lowest priority a log call can have), so it stays a separate choice from Verbose in the
     * dropdown and in the saved settings -- and still includes everything if a lower priority
     * were ever added.
     */
    static final int LEVEL_ALL = 0;
    /** Event level meaning "send nothing to the server". */
    static final int LEVEL_NONE = 100;
    /** The choices in the UI's Event Level dropdown, lowest (most verbose) first. */
    static final int[] LEVELS = {LEVEL_ALL, Log.VERBOSE, Log.DEBUG, Log.INFO, Log.WARN, Log.ERROR, LEVEL_NONE};
    static final String[] LEVEL_LABELS = {"All", "Verbose", "Debug", "Info", "Warn", "Error", "None (send nothing)"};

    private LogConfig() {
    }

    /** One consistent snapshot of the settings. */
    static final class Settings {
        /** Also write every log line to logcat -- at every level, regardless of eventLevel. */
        final boolean logPrint;
        /** Lowest level sent to the server (one of LEVELS); lower-level events are not sent. */
        final int eventLevel;
        /** Normalized ws:// or wss:// address of the dashboard server's /logs endpoint. */
        final String serverUrl;
        /** The forwarder reads the demo application's "PerformanceMetrics" log lines and sends them to the server. */
        final boolean perfLog;

        Settings(boolean logPrint, int eventLevel, String serverUrl, boolean perfLog) {
            this.logPrint = logPrint;
            this.eventLevel = eventLevel;
            this.serverUrl = serverUrl;
            this.perfLog = perfLog;
        }
    }

    /** Index of a level in LEVELS / LEVEL_LABELS (for the dropdown); the default's if unknown. */
    static int levelToIndex(int level) {
        for (int i = 0; i < LEVELS.length; i++) {
            if (LEVELS[i] == level) {
                return i;
            }
        }
        return levelToIndex(DEFAULT_EVENT_LEVEL);
    }

    /** Reads the saved settings -- or the defaults for anything missing or unreadable. */
    static Settings read(Context context) {
        Properties properties = new Properties();
        try (InputStream in = settingsFile(context).openRead()) {
            properties.load(in);
        } catch (IOException e) {
            // First run (no file yet) or unreadable: fall through to the defaults.
        }
        boolean logPrint = properties.containsKey(KEY_LOG_PRINT)
                ? Boolean.parseBoolean(properties.getProperty(KEY_LOG_PRINT)) : DEFAULT_LOG_PRINT;
        int eventLevel = DEFAULT_EVENT_LEVEL;
        try {
            int saved = Integer.parseInt(properties.getProperty(KEY_EVENT_LEVEL, ""));
            eventLevel = LEVELS[levelToIndex(saved)];
        } catch (NumberFormatException ignored) {
            // Not saved yet: keep the default.
        }
        String serverUrl = normalizeServerUrl(properties.getProperty(KEY_SERVER_URL));
        boolean perfLog = properties.containsKey(KEY_PERF_LOG)
                ? Boolean.parseBoolean(properties.getProperty(KEY_PERF_LOG)) : DEFAULT_PERF_LOG;
        return new Settings(logPrint, eventLevel, serverUrl != null ? serverUrl : DEFAULT_SERVER_URL, perfLog);
    }

    /** Saves the settings; the file is replaced atomically, so a reader never sees half of it. */
    static void write(Context context, Settings settings) {
        Properties properties = new Properties();
        properties.setProperty(KEY_LOG_PRINT, String.valueOf(settings.logPrint));
        properties.setProperty(KEY_EVENT_LEVEL, String.valueOf(settings.eventLevel));
        properties.setProperty(KEY_SERVER_URL, settings.serverUrl);
        properties.setProperty(KEY_PERF_LOG, String.valueOf(settings.perfLog));

        AtomicFile file = settingsFile(context);
        FileOutputStream out = null;
        try {
            out = file.startWrite();
            properties.store(out, null);
            file.finishWrite(out);
        } catch (IOException e) {
            if (out != null) {
                file.failWrite(out);
            }
            Log.e(TAG, "Could not save the log settings", e);
        }
    }

    /** Tells the already-running service and forwarder processes about new settings. */
    static void broadcast(Context context, Settings settings) {
        context.sendBroadcast(new Intent(ACTION_SETTINGS_CHANGED)
                .setPackage(context.getPackageName())
                .putExtra(EXTRA_LOG_PRINT, settings.logPrint)
                .putExtra(EXTRA_EVENT_LEVEL, settings.eventLevel)
                .putExtra(EXTRA_SERVER_URL, settings.serverUrl)
                .putExtra(EXTRA_PERF_LOG, settings.perfLog));
    }

    /** Settings carried by an ACTION_SETTINGS_CHANGED broadcast; null if it's incomplete. */
    static Settings fromIntent(Intent intent) {
        String serverUrl = intent.getStringExtra(EXTRA_SERVER_URL);
        if (serverUrl == null || !intent.hasExtra(EXTRA_LOG_PRINT) || !intent.hasExtra(EXTRA_EVENT_LEVEL)) {
            return null;
        }
        return new Settings(intent.getBooleanExtra(EXTRA_LOG_PRINT, DEFAULT_LOG_PRINT),
                intent.getIntExtra(EXTRA_EVENT_LEVEL, DEFAULT_EVENT_LEVEL), serverUrl,
                intent.getBooleanExtra(EXTRA_PERF_LOG, DEFAULT_PERF_LOG));
    }

    /**
     * Cleans up what the user typed into the server address field: "host:port" becomes
     * "ws://host:port/logs", http(s):// becomes ws(s)://, and a missing path becomes "/logs".
     * Returns null if it isn't a usable address (empty, another scheme, or no host).
     */
    static String normalizeServerUrl(String input) {
        if (input == null) {
            return null;
        }
        String text = input.trim();
        if (text.isEmpty()) {
            return null;
        }
        String lower = text.toLowerCase(Locale.ROOT);
        if (lower.startsWith("http://")) {
            text = "ws://" + text.substring("http://".length());
        } else if (lower.startsWith("https://")) {
            text = "wss://" + text.substring("https://".length());
        } else if (!lower.startsWith("ws://") && !lower.startsWith("wss://")) {
            if (text.contains("://")) {
                return null;
            }
            text = "ws://" + text;
        }
        try {
            URI uri = new URI(text);
            int port = uri.getPort();
            if (uri.getHost() == null) {
                // java.net.URI rejects some names real networks use, such as "pc_name": fall back
                // to a plain host[:port] check of the authority.
                String authority = uri.getRawAuthority();
                Matcher plainHost = PLAIN_AUTHORITY.matcher(authority == null ? "" : authority);
                if (!plainHost.matches()) {
                    return null;
                }
                port = plainHost.group(2) == null ? -1 : Integer.parseInt(plainHost.group(2));
            }
            if (port != -1 && (port < 1 || port > 65535)) {
                return null;
            }
            String path = uri.getRawPath();
            String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
            if (path == null || path.isEmpty() || path.equals("/")) {
                String query = uri.getRawQuery();
                return scheme + "://" + uri.getRawAuthority() + "/logs" + (query != null ? "?" + query : "");
            }
            return scheme + text.substring(uri.getScheme().length());
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private static AtomicFile settingsFile(Context context) {
        return new AtomicFile(new File(context.getFilesDir(), SETTINGS_FILE_NAME));
    }
}
