package com.zebra.ai.distributed;

import java.util.Locale;

/**
 * Builds JSON text without any library, so the device-status message (see DeviceStatus.toJson())
 * can be produced -- and tested -- anywhere, including in plain JVM unit tests. Methods chain; a
 * key must be written before every value inside an object, and never inside an array:
 *
 *   new JsonText().beginObject().key("battery").num(54).key("tags").beginArray().str("a").endArray().endObject()
 *   -> {"battery":54,"tags":["a"]}
 *
 * Numbers that JSON cannot express (NaN, infinity) are written as null.
 */
final class JsonText {

    private static final int MAX_DEPTH = 16;

    private final StringBuilder mOut = new StringBuilder(1024);
    /** Per open object/array: has it got a first element yet (so the next one needs a comma). */
    private final boolean[] mHasItems = new boolean[MAX_DEPTH];
    private int mDepth = -1;
    /** A key was just written: the value that follows must not be preceded by a comma. */
    private boolean mAfterKey;

    JsonText beginObject() {
        beforeValue();
        mOut.append('{');
        push();
        return this;
    }

    JsonText endObject() {
        mOut.append('}');
        pop();
        return this;
    }

    JsonText beginArray() {
        beforeValue();
        mOut.append('[');
        push();
        return this;
    }

    JsonText endArray() {
        mOut.append(']');
        pop();
        return this;
    }

    JsonText key(String name) {
        if (mDepth < 0) {
            throw new IllegalStateException("key outside an object");
        }
        if (mHasItems[mDepth]) {
            mOut.append(',');
        }
        mHasItems[mDepth] = true;
        appendQuoted(name);
        mOut.append(':');
        mAfterKey = true;
        return this;
    }

    /** A string value; null is written as JSON null. */
    JsonText str(String value) {
        beforeValue();
        if (value == null) {
            mOut.append("null");
        } else {
            appendQuoted(value);
        }
        return this;
    }

    JsonText num(long value) {
        beforeValue();
        mOut.append(value);
        return this;
    }

    /** A number with a fixed count of decimals; NaN and infinity become null. */
    JsonText num(double value, int decimals) {
        beforeValue();
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            mOut.append("null");
        } else {
            mOut.append(String.format(Locale.US, "%." + decimals + "f", value));
        }
        return this;
    }

    JsonText bool(boolean value) {
        beforeValue();
        mOut.append(value ? "true" : "false");
        return this;
    }

    JsonText nul() {
        beforeValue();
        mOut.append("null");
        return this;
    }

    @Override
    public String toString() {
        return mOut.toString();
    }

    private void push() {
        if (mDepth + 1 >= MAX_DEPTH) {
            throw new IllegalStateException("JSON nested too deeply");
        }
        mDepth++;
        mHasItems[mDepth] = false;
    }

    private void pop() {
        if (mDepth < 0) {
            throw new IllegalStateException("unbalanced end");
        }
        mDepth--;
    }

    private void beforeValue() {
        if (mAfterKey) {
            mAfterKey = false;
            return;
        }
        if (mDepth >= 0) {
            if (mHasItems[mDepth]) {
                mOut.append(',');
            }
            mHasItems[mDepth] = true;
        }
    }

    private void appendQuoted(String text) {
        mOut.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"':
                    mOut.append("\\\"");
                    break;
                case '\\':
                    mOut.append("\\\\");
                    break;
                case '\n':
                    mOut.append("\\n");
                    break;
                case '\r':
                    mOut.append("\\r");
                    break;
                case '\t':
                    mOut.append("\\t");
                    break;
                case '\b':
                    mOut.append("\\b");
                    break;
                case '\f':
                    mOut.append("\\f");
                    break;
                default:
                    if (c < 0x20) {
                        mOut.append(String.format(Locale.US, "\\u%04x", (int) c));
                    } else {
                        mOut.append(c);
                    }
            }
        }
        mOut.append('"');
    }
}
