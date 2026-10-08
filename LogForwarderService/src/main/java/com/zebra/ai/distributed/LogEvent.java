package com.zebra.ai.distributed;

import android.os.Parcel;
import android.os.Parcelable;

/**
 * One log line, captured by EventLog in the service process at the moment of the call and
 * carried to the log-forwarder process in a batch (see LogSink). Holds everything needed to
 * rebuild a "logcat -v threadtime" line there, so the timestamp and thread are those of the code
 * that logged -- not of whichever thread later sent it.
 */
public final class LogEvent implements Parcelable {

    /** System.currentTimeMillis() at the log call. */
    final long timeMillis;
    /** android.util.Log priority: VERBOSE=2, DEBUG=3, INFO=4, WARN=5, ERROR=6. */
    final int level;
    final int pid;
    /** Linux thread id of the logging thread -- the TID column in logcat. */
    final int tid;
    final String tag;
    /** The message, with the stack trace appended on following lines when one was logged. */
    final String message;

    LogEvent(long timeMillis, int level, int pid, int tid, String tag, String message) {
        this.timeMillis = timeMillis;
        this.level = level;
        this.pid = pid;
        this.tid = tid;
        this.tag = tag;
        this.message = message;
    }

    /** Rough size of this event in a Parcel (strings are written as UTF-16), for batch sizing. */
    int estimatedBytes() {
        return 40 + 2 * (tag.length() + message.length());
    }

    private LogEvent(Parcel in) {
        timeMillis = in.readLong();
        level = in.readInt();
        pid = in.readInt();
        tid = in.readInt();
        tag = in.readString();
        message = in.readString();
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeLong(timeMillis);
        dest.writeInt(level);
        dest.writeInt(pid);
        dest.writeInt(tid);
        dest.writeString(tag);
        dest.writeString(message);
    }

    @Override
    public int describeContents() {
        return 0;
    }

    public static final Creator<LogEvent> CREATOR = new Creator<>() {
        @Override
        public LogEvent createFromParcel(Parcel in) {
            return new LogEvent(in);
        }

        @Override
        public LogEvent[] newArray(int size) {
            return new LogEvent[size];
        }
    };
}
