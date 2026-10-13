package com.zebra.ai.distributed;

/**
 * Tells what the Zebra scanner (DataWedge, "keystroke output") typed from what a person types, on the key events the screen receives.
 *
 * A scanner types a whole code within a few milliseconds -- one key event per character a few ms apart, or the whole text in one
 * event -- where a person needs a tenth of a second or more per key. So a run of characters that arrives fast enough, and is long
 * enough, is one scan; it is complete when nothing more has come for SETTLE_MILLIS. Plain Java, no Android: it is tested on its own.
 *
 * The caller feeds it every printable key (onChar), every whole-string key event (onChunk) and every Enter, with the time of the
 * event, and asks for the finished scan with finish().
 */
final class ScanCapture {

    /** The longest pause between two characters of one scan; a longer one ends the run (a person typing is slower than this). */
    static final long MAX_GAP_MILLIS = 120;
    /** Nothing more for this long after the last character: the scan is complete (its Enter, if there is one, has come by then). */
    static final long SETTLE_MILLIS = 300;
    /** Fewer characters than this are never taken for a scan. */
    static final int MIN_CHARS = 4;
    /** Typing slower than this, on average, is a person's: a scanner is far faster (a few ms per character). */
    static final long MAX_MEAN_GAP_MILLIS = 60;

    private final StringBuilder text = new StringBuilder();
    private long firstAt;
    private long lastAt;
    private int chars;
    private boolean chunk;

    /** True while a run of characters is going on: the last one came less than MAX_GAP_MILLIS ago. */
    boolean isRunning(long now) {
        return chars > 0 && now - lastAt <= MAX_GAP_MILLIS;
    }

    /** True if nothing is held at all (no run started, or it has been taken by finish()). */
    boolean isEmpty() {
        return chars == 0;
    }

    /** A character typed (a key event). Returns true if it begins a new run (the caller keeps what the field said before it). */
    boolean onChar(char c, long now) {
        boolean begins = begin(now);
        text.append(c);
        chars++;
        lastAt = now;
        return begins;
    }

    /** A whole string in one key event (what DataWedge sends when it does not send the characters one by one). */
    boolean onChunk(String s, long now) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        boolean begins = begin(now);
        text.append(s);
        chars += s.length();
        chunk = true;
        lastAt = now;
        return begins;
    }

    private boolean begin(long now) {
        boolean begins = chars == 0 || now - lastAt > MAX_GAP_MILLIS;
        if (begins) {
            text.setLength(0);
            chars = 0;
            chunk = false;
            firstAt = now;
        }
        return begins;
    }

    /** True if what is held so far is long enough and fast enough to be a scan. */
    boolean looksLikeScan() {
        if (chars < MIN_CHARS) {
            return false;
        }
        if (chunk) {
            return true;
        }
        return (lastAt - firstAt) <= MAX_MEAN_GAP_MILLIS * (chars - 1L);
    }

    /**
     * An Enter key now: true if it belongs to a scan that is going on (a scanner can end its code with one), so the caller swallows it
     * -- it must not press the button that has the focus, or "Done" on a half-finished field. A person's Enter is not taken.
     */
    boolean takesEnter(long now) {
        return isRunning(now) && looksLikeScan();
    }

    /** True once the scan is complete: a run is held and nothing has come for SETTLE_MILLIS. */
    boolean isComplete(long now) {
        return chars > 0 && now - lastAt >= SETTLE_MILLIS;
    }

    /**
     * Takes what was typed if it was a scan (and is complete); null if it was a person typing -- and in both cases the run is over.
     * Called a moment after the last character; while the run is still going it returns null and keeps everything.
     */
    String finish(long now) {
        if (!isComplete(now)) {
            return null;
        }
        String scan = looksLikeScan() ? text.toString() : null;
        text.setLength(0);
        chars = 0;
        chunk = false;
        return scan;
    }

    /** Forgets what is held (the screen is leaving). */
    void reset() {
        text.setLength(0);
        chars = 0;
        chunk = false;
    }
}
