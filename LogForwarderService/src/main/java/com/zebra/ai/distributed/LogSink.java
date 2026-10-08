package com.zebra.ai.distributed;

import android.os.Binder;
import android.os.IBinder;
import android.os.Parcel;
import android.os.RemoteException;

import java.util.List;

/**
 * The link between EventLog (service process) and LogForwarderService (":log_forwarder" process):
 * two "oneway" Binder transactions -- one carries a batch of LogEvents, the other one device-status
 * message (see DeviceMonitor). oneway means the caller never waits for the forwarder: the call
 * returns as soon as the message is handed to the Binder driver, and messages from one caller
 * arrive in order.
 *
 * Written by hand -- what an AIDL oneway method with an "in List" parameter generates -- instead
 * of as an .aidl file, because of the leftover srcDirs entry in build.gradle that adds
 * ".../aidl_source_output_dir/release/out" as an extra Java source folder. With an .aidl file,
 * the interface generated for the release variant lands in that folder and is compiled a second
 * time next to the debug variant's own copy: "duplicate class", and in the IDE "Incompatible
 * types: found X, required X" for the same interface. With no generated class there is only one
 * definition of anything here, whatever variants have been built.
 */
final class LogSink {

    private static final String DESCRIPTOR = "com.zebra.ai.distributed.LogSink";
    private static final int TRANSACTION_ON_EVENTS = IBinder.FIRST_CALL_TRANSACTION;
    private static final int TRANSACTION_ON_DEVICE_STATUS = IBinder.FIRST_CALL_TRANSACTION + 1;

    private LogSink() {
    }

    /** What the forwarder does with what it receives. */
    interface Receiver {
        /** Runs on a Binder thread, so it must only queue the events, not process them. */
        void onEvents(List<LogEvent> events);

        /**
         * One complete device-status message (JSON, see DeviceStatus.toJson()), to be sent to the
         * server as it is. Runs on a Binder thread, so it must only remember it.
         */
        void onDeviceStatus(String json);
    }

    /** The forwarder's end: the IBinder LogForwarderService.onBind() hands out. */
    static final class Server extends Binder {
        private final Receiver mReceiver;

        Server(Receiver receiver) {
            mReceiver = receiver;
        }

        @Override
        protected boolean onTransact(int code, Parcel data, Parcel reply, int flags) throws RemoteException {
            if (code == TRANSACTION_ON_EVENTS) {
                data.enforceInterface(DESCRIPTOR);
                List<LogEvent> events = data.createTypedArrayList(LogEvent.CREATOR);
                if (events != null) {
                    mReceiver.onEvents(events);
                }
                return true;
            }
            if (code == TRANSACTION_ON_DEVICE_STATUS) {
                data.enforceInterface(DESCRIPTOR);
                String json = data.readString();
                if (json != null) {
                    mReceiver.onDeviceStatus(json);
                }
                return true;
            }
            return super.onTransact(code, data, reply, flags);
        }
    }

    /** The service's end: wraps the IBinder that bindService() delivered. */
    static final class Client {
        private final IBinder mRemote;

        Client(IBinder remote) {
            mRemote = remote;
        }

        /**
         * Sends a batch. The events are copied into the transaction before this returns, so the
         * caller may reuse the list. Throws RemoteException if the forwarder's process is gone.
         */
        void onEvents(List<LogEvent> events) throws RemoteException {
            Parcel data = Parcel.obtain();
            try {
                data.writeInterfaceToken(DESCRIPTOR);
                data.writeTypedList(events);
                mRemote.transact(TRANSACTION_ON_EVENTS, data, null, IBinder.FLAG_ONEWAY);
            } finally {
                data.recycle();
            }
        }

        /**
         * Sends one device-status message (a few KB of JSON at most -- far below Binder's limit).
         * Throws RemoteException if the forwarder's process is gone.
         */
        void onDeviceStatus(String json) throws RemoteException {
            Parcel data = Parcel.obtain();
            try {
                data.writeInterfaceToken(DESCRIPTOR);
                data.writeString(json);
                mRemote.transact(TRANSACTION_ON_DEVICE_STATUS, data, null, IBinder.FLAG_ONEWAY);
            } finally {
                data.recycle();
            }
        }
    }
}
