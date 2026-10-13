package com.zebra.ai.distributed;

import java.util.Locale;

/**
 * What a scanned code has to be to be taken as the log server address: the text of the dashboard's QR code, "ws://10.117.131.123:5000/logs".
 * Plain Java (LogConfig.normalizeServerUrl() is a static function), so it is tested on its own.
 */
final class ScannedAddress {

    private ScannedAddress() {
    }

    /**
     * The address in the scanned text, in the form it is saved in, or null if the code is not a log server address.
     *
     * The scanner may add a line break or other control characters to the text: they are dropped. Only "ws://" and "wss://" are taken --
     * LogConfig.normalizeServerUrl() is kind to what a person types ("host:port", "http://..."), which for a scan would turn the QR code of
     * any web page or product into a "server address".
     */
    static String parse(String scanned) {
        if (scanned == null) {
            return null;
        }
        StringBuilder clean = new StringBuilder(scanned.length());
        for (int i = 0; i < scanned.length(); i++) {
            char c = scanned.charAt(i);
            if (!Character.isISOControl(c)) {
                clean.append(c);
            }
        }
        String text = clean.toString().trim();
        String lower = text.toLowerCase(Locale.ROOT);
        if (!lower.startsWith("ws://") && !lower.startsWith("wss://")) {
            return null;
        }
        if (text.indexOf(' ') >= 0) {
            return null;
        }
        return LogConfig.normalizeServerUrl(text);
    }
}
