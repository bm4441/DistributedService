package com.zebra.ai.distributed;

import android.app.ActivityManager;
import android.content.Context;
import android.net.LinkAddress;
import android.net.LinkProperties;
import android.net.Network;
import android.net.wifi.WifiManager;
import android.os.BatteryManager;
import android.os.IBinder;
import android.os.PowerManager;
import android.os.RemoteException;
import android.os.SystemClock;
import android.util.Log;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Multicast peer-discovery protocol: joining/leaving the multicast group, the receive loop,
 * the wire MSG/ACK protocol, and the per-request discovery bookkeeping (PendingRequest) and
 * long-lived IPeerDiscoveryListener registrations. Split out of DistributedService verbatim --
 * see DistributedService's own fields/methods for the shared state this reaches into (mService).
 */
final class MulticastDiscoveryEngine {

    private static final String TAG = "DistributedService";

    /* ================= Multicast Discovery Protocol Constants ================= */
    private static final int DISCOVERY_PORT = 58231;
    private static final String MULTICAST_GROUP_ADDRESS = "224.0.0.251";
    private static final String PROTOCOL_TAG = "SRJANA";
    private static final String TYPE_MSG = "MSG";
    private static final String TYPE_ACK = "ACK";
    private static final long CONFIRMATION_WINDOW_MILLIS = 3000L;
    private static final int MAX_PACKET_BYTES = 512;
    private static final int SOCKET_BUFFER_BYTES = 512 * 1024; // 512 KB buffer

    /* ================= IPeerDiscoveryListener error codes ================= */
    private static final int ERROR_WIFI_UNAVAILABLE = 1;
    private static final int ERROR_DISCOVERY_SOCKET_UNAVAILABLE = 2;
    private static final int ERROR_MULTICAST_SEND_FAILED = 3;
    static final int ERROR_SERVICE_PAUSED = 4;

    // TODO: derive from real device/role configuration once that exists; 1 = CONTRIBUTOR/REQUESTOR
    // (per Service_AISDK_Distributed_service_basic_flow's nodeRole values) is a reasonable
    // generic default for a peer that hasn't been specifically configured.
    private static final int LOCAL_NODE_ROLE = 1;

    /** How long the peers found by a peer discovery count as fresh: getConnectedPeers() starts a new discovery after this. */
    static final long PEER_DISCOVERY_AGE_MILLIS = 5_000L;

    // When the last peer discovery was started (SystemClock.elapsedRealtime(); 0 = never), and for which capability and
    // listener -- see refreshPeersIfExpired().
    private final AtomicLong mLastPeerDiscoveryMillis = new AtomicLong();
    private volatile String mLastDiscoveryCapability;
    private volatile IPeerDiscoveryListener mLastDiscoveryListener;

    final ConcurrentHashMap<String, PendingRequest> mPendingRequests = new ConcurrentHashMap<>();
    // Long-lived listeners registered via registerDiscoveryListener(), independent of any single
    // startPeerDiscovery() request -- notified of events like onPeerReady() as they happen.
    final CopyOnWriteArrayList<IPeerDiscoveryListener> mRegisteredListeners = new CopyOnWriteArrayList<>();
    private final Object mSocketLock = new Object();

    private volatile MulticastSocket mDiscoverySocket;
    // A Requestor's socket: discovery requests go out from it and the ACKs come back to it. It is NOT joined to the multicast
    // group -- a Requestor does not listen for other devices' requests (see startRequestSocketLocked()).
    private volatile MulticastSocket mRequestSocket;
    private volatile WifiManager.MulticastLock mMulticastLock;
    private volatile WifiManager.WifiLock mWifiLock;
    private volatile PowerManager.WakeLock mWakeLock;
    private final AtomicBoolean mIsListening = new AtomicBoolean(false);

    private final DistributedService svc;

    MulticastDiscoveryEngine(DistributedService svc) {
        this.svc = svc;
    }

    boolean isListening() {
        return mIsListening.get();
    }

    void startMulticastListener(Network network) {
        synchronized (mSocketLock) {
            if (!svc.isServiceActive()) {
                EventLog.i(TAG, "Not opening the discovery sockets: the service is paused by its policy");
                return;
            }
            if (svc.getServiceMode() != ServiceMode.CONTRIBUTOR) {
                // Only a Contributor listens for discovery requests. A Requestor is not joined to the group: it only has
                // what it needs to send its own requests and receive the answers.
                startRequestSocketLocked(network);
                return;
            }
            if (mIsListening.get() && mDiscoverySocket != null && !mDiscoverySocket.isClosed()) {
                EventLog.i(TAG, "Multicast listener already active on port " + DISCOVERY_PORT + ", skipping re-bind.");
                return;
            }
            stopMulticastListener();
            EventLog.i(TAG, "Starting high-performance multicast listener...");

            PowerManager pm = svc.getSystemService(PowerManager.class);
            if (pm != null) {
                mWakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, TAG + ":MulticastWakeLock");
                mWakeLock.setReferenceCounted(false);
                mWakeLock.acquire();
            }

            WifiManager wifiManager = (WifiManager) svc.getApplicationContext().getSystemService(Context.WIFI_SERVICE);
            if (wifiManager != null) {
                // Low-latency mode: the radio stays awake and answers at once, which shortens every task upload and result send.
                // (WIFI_MODE_FULL_HIGH_PERF is deprecated since API 34 and no longer has an effect.)
                mWifiLock = wifiManager.createWifiLock(WifiManager.WIFI_MODE_FULL_LOW_LATENCY, TAG + ":WifiLock");
                mWifiLock.setReferenceCounted(false);
                mWifiLock.acquire();

                mMulticastLock = wifiManager.createMulticastLock(TAG + ":MulticastLock");
                mMulticastLock.setReferenceCounted(false);
                mMulticastLock.acquire();
            }

            try {
                MulticastSocket socket = new MulticastSocket(DISCOVERY_PORT);
                socket.setReuseAddress(true);
                socket.setReceiveBufferSize(SOCKET_BUFFER_BYTES);
                socket.setSendBufferSize(SOCKET_BUFFER_BYTES);

                network.bindSocket(socket);

                NetworkInterface nif = getNetworkInterfaceForNetwork(network);
                if (nif != null) {
                    socket.setNetworkInterface(nif);
                }

                InetAddress group = InetAddress.getByName(MULTICAST_GROUP_ADDRESS);
                socket.joinGroup(new InetSocketAddress(group, DISCOVERY_PORT), nif);
                socket.setTimeToLive(5);

                mDiscoverySocket = socket;
                mIsListening.set(true);

                EventLog.i(TAG, "Multicast joined on group " + MULTICAST_GROUP_ADDRESS
                        + " interface " + (nif != null ? nif.getName() : "default")
                        + " port " + DISCOVERY_PORT);

                Thread listenerThread = new Thread(this::runReceiveLoop, "SrjanaMulticastListener");
                listenerThread.setPriority(Thread.MAX_PRIORITY);
                listenerThread.start();
            } catch (IOException e) {
                EventLog.e(TAG, "Failed to start multicast listener", e);
                stopMulticastListener();
            }
        }
    }

    /** Closes everything this device has open for discovery: the listener (Contributor) and the request socket (Requestor). */
    void stopMulticastListener() {
        synchronized (mSocketLock) {
            stopListenerLocked();
            stopRequestSocketLocked();
        }
    }

    /** Leaves the multicast group and closes the listener, with the locks it needed. Caller holds mSocketLock. */
    private void stopListenerLocked() {
        mIsListening.set(false);
        MulticastSocket socket = mDiscoverySocket;
        mDiscoverySocket = null;

        if (socket != null && !socket.isClosed()) {
            try {
                InetAddress group = InetAddress.getByName(MULTICAST_GROUP_ADDRESS);
                socket.leaveGroup(new InetSocketAddress(group, DISCOVERY_PORT), null);
            } catch (Exception ignored) {
            } finally {
                socket.close();
            }
            EventLog.i(TAG, "Multicast listener socket closed.");
        }

        if (mMulticastLock != null && mMulticastLock.isHeld()) {
            mMulticastLock.release();
            mMulticastLock = null;
        }
        if (mWifiLock != null && mWifiLock.isHeld()) {
            mWifiLock.release();
            mWifiLock = null;
        }
        if (mWakeLock != null && mWakeLock.isHeld()) {
            mWakeLock.release();
            mWakeLock = null;
        }
    }

    /** Closes the Requestor's socket, if there is one. Caller holds mSocketLock. */
    private void stopRequestSocketLocked() {
        MulticastSocket socket = mRequestSocket;
        mRequestSocket = null;
        if (socket != null && !socket.isClosed()) {
            socket.close();
            EventLog.i(TAG, "Discovery request socket closed.");
        }
    }

    /**
     * What a Requestor has open instead of the listener: an ordinary socket on a free port, bound to the Wi-Fi network, NOT
     * joined to the multicast group. Discovery requests are sent from it to the group, and a contributor answers a request by
     * sending its ACK to the request's source address and port -- so the ACKs come back to this socket, while the requests of
     * other devices, which go to the group and port 58231, never reach this device. A Requestor also holds none of the wake,
     * Wi-Fi and multicast locks the listener needs. Caller holds mSocketLock.
     */
    private void startRequestSocketLocked(Network network) {
        stopListenerLocked();                          // a Requestor does not listen
        if (!svc.isServiceActive()) {
            return;                                    // paused by the service policy: nothing is opened
        }
        MulticastSocket existing = mRequestSocket;
        if (existing != null && !existing.isClosed()) {
            return;
        }
        try {
            MulticastSocket socket = new MulticastSocket((java.net.SocketAddress) null);
            socket.bind(new InetSocketAddress(0));
            socket.setReceiveBufferSize(SOCKET_BUFFER_BYTES);
            network.bindSocket(socket);
            NetworkInterface nif = getNetworkInterfaceForNetwork(network);
            if (nif != null) {
                socket.setNetworkInterface(nif);       // the interface the requests go out of
            }
            socket.setTimeToLive(5);
            mRequestSocket = socket;
            EventLog.i(TAG, "Requestor mode: not listening for discovery requests; this device's own requests go out from port "
                    + socket.getLocalPort() + " and the answers come back to it");
            Thread receiver = new Thread(() -> runRequestReceiveLoop(socket), "SrjanaDiscoveryRequester");
            receiver.start();
        } catch (IOException e) {
            EventLog.e(TAG, "Failed to open the discovery request socket", e);
        }
    }

    /**
     * The service's mode changed (see DistributedService.applicationBound()/applicationUnbound()): a Requestor stops
     * listening and opens its request socket; a Contributor closes the request socket and starts listening again -- unless it
     * is reserved, which keeps it undiscoverable until its requestor releases it. Does nothing without Wi-Fi: nothing is open
     * then, and the next Wi-Fi connection opens what the mode needs (startMulticastListener()).
     */
    void applyServiceMode() {
        synchronized (mSocketLock) {
            Network network = svc.mWifiNetwork;
            if (network == null) {
                return;
            }
            if (svc.getServiceMode() == ServiceMode.CONTRIBUTOR) {
                stopRequestSocketLocked();
                if (svc.mReadinessEngine.getReservedFor() == null) {
                    startMulticastListener(network);
                }
            } else {
                startRequestSocketLocked(network);
            }
        }
    }

    /** The socket a discovery request goes out from: a Requestor's own, opened now if it is not yet; a Contributor's listener. */
    private MulticastSocket socketForRequest() {
        synchronized (mSocketLock) {
            if (svc.getServiceMode() == ServiceMode.CONTRIBUTOR) {
                return mDiscoverySocket;
            }
            MulticastSocket socket = mRequestSocket;
            Network network = svc.mWifiNetwork;
            if ((socket == null || socket.isClosed()) && network != null) {
                startRequestSocketLocked(network);
                socket = mRequestSocket;
            }
            return socket;
        }
    }

    /** Receives the ACKs to this device's requests on the Requestor's socket, until it is closed. */
    private void runRequestReceiveLoop(MulticastSocket socket) {
        byte[] buffer = new byte[MAX_PACKET_BYTES];
        while (!socket.isClosed()) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (IOException e) {
                if (socket.isClosed()) break;
                EventLog.w(TAG, "Request socket receive exception: " + e.getMessage());
                continue;
            }
            handleIncomingPacket(packet);
        }
    }

    private void runReceiveLoop() {
        MulticastSocket socket = mDiscoverySocket;
        if (socket == null) return;

        byte[] buffer = new byte[MAX_PACKET_BYTES];
        EventLog.i(TAG, "Discovery receive loop running.");

        while (mIsListening.get() && !socket.isClosed()) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (IOException e) {
                if (!mIsListening.get()) break;
                EventLog.w(TAG, "Receive loop exception: " + e.getMessage());
                continue;
            }

            handleIncomingPacket(packet);
        }
        EventLog.i(TAG, "Discovery receive loop terminated.");
    }

    private void handleIncomingPacket(DatagramPacket packet) {
        String payload = new String(packet.getData(), packet.getOffset(), packet.getLength(), StandardCharsets.UTF_8);

        int firstPipe = payload.indexOf('|');
        if (firstPipe == -1) return;

        String protocol = payload.substring(0, firstPipe);
        if (!PROTOCOL_TAG.equals(protocol)) return;

        int secondPipe = payload.indexOf('|', firstPipe + 1);
        if (secondPipe == -1) return;
        String type = payload.substring(firstPipe + 1, secondPipe);

        int thirdPipe = payload.indexOf('|', secondPipe + 1);
        if (thirdPipe == -1) return;
        String requestId = payload.substring(secondPipe + 1, thirdPipe);

        int fourthPipe = payload.indexOf('|', thirdPipe + 1);
        String serialNumber;
        String messageBody = "";
        if (fourthPipe == -1) {
            serialNumber = payload.substring(thirdPipe + 1);
        } else {
            serialNumber = payload.substring(thirdPipe + 1, fourthPipe);
            messageBody = payload.substring(fourthPipe + 1);
        }

        if (TYPE_MSG.equals(type)) {
            if (svc.getLocalSerialNumber().equals(serialNumber)) {
                return;
            }
            // Only a Contributor answers a discovery request, at any moment: a Requestor is not even listening (see
            // startRequestSocketLocked()), and one that is still reached -- the mode just changed -- is silent too.
            ServiceMode mode = svc.getServiceMode();
            if (mode != ServiceMode.CONTRIBUTOR) {
                EventLog.i(TAG, "Not answering the discovery request from " + serialNumber + " (req=" + requestId
                        + "): the service is in " + mode.label + " mode, not Contributor");
                return;
            }
            EventLog.i(TAG, "Received MSG from " + serialNumber + " (req=" + requestId + "): " + messageBody);

            InetAddress senderAddress = packet.getAddress();
            int senderPort = packet.getPort();
            Network currentNetwork = svc.mWifiNetwork;

            svc.mOutboundExecutor.execute(() -> {
                if (currentNetwork != null) {
                    EventLog.i(TAG, "Dispatching ACK to " + senderAddress + ":" + senderPort);
                    sendAck(requestId, senderAddress, senderPort);
                    // Check if active; if not, spin it up on this first MSG
                    if (!svc.mGrpcEngine.isTransferServerActive()) {
                        EventLog.e(TAG, "Transfer server Is not started; Restarting the Transfer server");
                        svc.mTransferExecutor.execute(svc.mGrpcEngine::startTransferServer);
                    }
                }
            });

        } else if (TYPE_ACK.equals(type)) {
            EventLog.i(TAG, "Received ACK from " + serialNumber + " (req=" + requestId + ")");
            InetAddress peerAddress = packet.getAddress();
            PeerInfo peerInfo = decodePeerInfo(serialNumber, peerAddress, messageBody);
            svc.mKnownDevices.put(serialNumber, peerInfo);

            PendingRequest pending = mPendingRequests.get(requestId);
            if (pending != null) {
                if (pending.respondingPeers.putIfAbsent(serialNumber, peerInfo) == null) {
                    // Info, not Debug: the dashboard server finds peers by matching this exact text
                    // (server.py PEER_FOUND_RE), and the default Event Level (Info) would otherwise
                    // never send it.
                    EventLog.i(TAG, "New peer responded to discovery: " + serialNumber);
                    // Before the application is told the peer joined: a peer that was reported lost and has answered
                    // again is not lost any more, so that it is reported again if it fails again.
                    svc.mReadinessEngine.peerJoined(serialNumber);
                    safeOnPeerJoined(pending.listener, peerInfo);
                }
            }
        }
    }

    private void sendAck(String requestId, InetAddress toAddress, int toPort) {
        String payload = PROTOCOL_TAG + "|" + TYPE_ACK + "|" + requestId + "|" + svc.getLocalSerialNumber()
                + "|" + encodePeerInfo(buildLocalPeerInfo());
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);

        try (DatagramSocket ackSocket = new DatagramSocket()) {
            if (svc.mWifiNetwork != null) {
                svc.mWifiNetwork.bindSocket(ackSocket);
            }
            DatagramPacket packet = new DatagramPacket(data, data.length, toAddress, toPort);
            ackSocket.send(packet);
            EventLog.i(TAG, "Dispatched instant ACK for req=" + requestId + " to " + toAddress + ":" + toPort);
        } catch (IOException e) {
            EventLog.e(TAG, "Failed to send ACK to " + toAddress + ":" + toPort, e);
        }
    }

    /**
     * Entry point for NetworkInterfaceServiceImpl.startPeerDiscovery(). Same trigger mechanism
     * the old ISrjanaService.sendMessage() used (multicast ping + confirmation window), but
     * status is now reported back through IPeerDiscoveryListener instead of a broadcast
     * callback list: onDiscoveryFailed() if discovery can't even start, onPeerJoined() as each
     * peer responds, onDiscoveryComplete() with the full list once the window closes.
     */
    void requestPeerDiscovery(String capability, IPeerDiscoveryListener listener) {
        // Remembered for refreshPeersIfExpired(), which has to start the same discovery again.
        mLastDiscoveryCapability = capability;
        mLastDiscoveryListener = listener;
        mLastPeerDiscoveryMillis.set(SystemClock.elapsedRealtime());
        if (!svc.isServiceActive()) {
            EventLog.w(TAG, "Peer discovery refused: the service is paused by its policy");
            safeOnDiscoveryFailed(listener, capability, ERROR_SERVICE_PAUSED, "The service is paused by its policy");
            return;
        }
        if (svc.mWifiNetwork == null) {
            EventLog.w(TAG, "Wi-Fi is not connected. Discovery aborted.");
            safeOnDiscoveryFailed(listener, capability, ERROR_WIFI_UNAVAILABLE, "Wi-Fi is not connected");
            return;
        }
        startMulticastRequest(capability, listener);
    }

    private void startMulticastRequest(String capability, IPeerDiscoveryListener listener) {
        MulticastSocket socket = socketForRequest();
        Network network = svc.mWifiNetwork;

        if (socket == null || socket.isClosed() || network == null) {
            EventLog.e(TAG, "Cannot broadcast: Network or Multicast socket is unavailable.");
            safeOnDiscoveryFailed(listener, capability, ERROR_DISCOVERY_SOCKET_UNAVAILABLE,
                    "Multicast discovery socket is unavailable");
            return;
        }

        String requestId = UUID.randomUUID().toString();
        mPendingRequests.put(requestId, new PendingRequest(capability, listener));

        String payload = PROTOCOL_TAG + "|" + TYPE_MSG + "|" + requestId + "|" + svc.getLocalSerialNumber() + "|" + capability;
        byte[] data = payload.getBytes(StandardCharsets.UTF_8);

        svc.mOutboundExecutor.execute(() -> {
            try {
                InetAddress group = InetAddress.getByName(MULTICAST_GROUP_ADDRESS);
                DatagramPacket packet = new DatagramPacket(data, data.length, group, DISCOVERY_PORT);
                socket.send(packet);
                EventLog.i(TAG, "Multicast request sent (id=" + requestId + ")");
            } catch (IOException e) {
                EventLog.e(TAG, "Failed to send multicast request", e);
                mPendingRequests.remove(requestId);
                safeOnDiscoveryFailed(listener, capability, ERROR_MULTICAST_SEND_FAILED,
                        "Failed to send discovery request: " + e.getMessage());
            }
        });

        svc.mScheduler.schedule(() -> completeRequest(requestId), CONFIRMATION_WINDOW_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * The service is paused by its policy: every discovery still waiting for its window to close is ended, and the
     * application is told it failed instead of waiting for answers that can no longer come.
     */
    void failPendingRequests(int errorCode, String message) {
        for (String requestId : new ArrayList<>(mPendingRequests.keySet())) {
            PendingRequest pending = mPendingRequests.remove(requestId);
            if (pending != null) {
                safeOnDiscoveryFailed(pending.listener, pending.capability, errorCode, message);
            }
        }
    }

    private void completeRequest(String requestId) {
        PendingRequest pending = mPendingRequests.remove(requestId);
        if (pending == null) return;

        EventLog.i(TAG, "Discovery window closed for " + requestId + ". Total responding peer(s): "
                + pending.respondingPeers.size());

        List<PeerInfo> peers = new ArrayList<>(pending.respondingPeers.values());
        safeOnDiscoveryComplete(pending.listener, pending.capability, peers);
    }

    /**
     * The application stopped its peer discovery (NetworkInterfaceServiceImpl.stopPeerDiscovery()): forgets the last
     * discovery -- its time, capability and listener -- so that getConnectedPeers() no longer starts one again for it
     * (see refreshPeersIfExpired()). Only when it is the discovery of this capability that was stopped: the application
     * can run discoveries for more than one capability, and stopping one must not end the refreshing of the last one
     * started for another.
     */
    void clearLastDiscovery(String capability) {
        String last = mLastDiscoveryCapability;
        if (last != null && !last.equals(capability)) {
            return;
        }
        mLastDiscoveryCapability = null;
        mLastDiscoveryListener = null;
        mLastPeerDiscoveryMillis.set(0);
    }

    /**
     * Called whenever the application asks for the connected peers (NetworkInterfaceServiceImpl.getConnectedPeers()):
     * if the last peer discovery was started PEER_DISCOVERY_AGE_MILLIS ago or more, starts one again -- the same one the
     * application started last (its capability and listener), so its answers come back the same way, onPeerJoined() and
     * onDiscoveryComplete(). Does not wait for it: the peers known now are returned by the caller at once. Does nothing
     * while the last discovery is younger than that, or when the application has not started a discovery yet (there is no
     * capability or listener to run one for) or is gone.
     */
    void refreshPeersIfExpired() {
        if (!svc.isServiceActive()) {
            return;                                    // paused by the service policy: no discovery is started
        }
        String capability = mLastDiscoveryCapability;
        IPeerDiscoveryListener listener = mLastDiscoveryListener;
        if (capability == null || listener == null) {
            Log.d(TAG, "Capability or Listner is null");
            return;
        }
        IBinder binder = listener.asBinder();
        if (binder != null && !binder.isBinderAlive()) {
            Log.d(TAG, "Binder is null");
            return;                                    // the application that asked is gone
        }
        long now = SystemClock.elapsedRealtime();
        long last = mLastPeerDiscoveryMillis.get();
        long age = now - last;
        if (age < PEER_DISCOVERY_AGE_MILLIS) {
            Log.d(TAG, "Age is not expired");
            return;
        }
        // Several callers may find it expired together: only the one that records the new time starts the discovery.
        if (!mLastPeerDiscoveryMillis.compareAndSet(last, now)) {
            return;
        }
        EventLog.i(TAG, "The last peer discovery is " + age + " ms old (peer discovery age " + PEER_DISCOVERY_AGE_MILLIS
                + " ms): starting peer discovery for capability '" + capability + "'");
        requestPeerDiscovery(capability, listener);
    }

    /**
     * Entry point for NetworkInterfaceServiceImpl.getPeerCount() / getConnectedPeers(). Returns
     * a snapshot of every peer that has ever ACKed a discovery request (mKnownDevices is never
     * pruned as peers go stale/disconnect, so this is "known", not necessarily "currently
     * reachable").
     */
    List<PeerInfo> getKnownPeers() {
        return new ArrayList<>(svc.mKnownDevices.values());
    }

    /**
     * Adds a long-lived discovery listener (see NetworkInterfaceServiceImpl.
     * registerDiscoveryListener()) -- notified of events like onPeerReady() independent of any
     * single startPeerDiscovery() request. De-duplicated by Binder identity, since a remote
     * IPeerDiscoveryListener proxy has no meaningful equals() of its own.
     */
    void registerDiscoveryListener(IPeerDiscoveryListener listener) {
        if (listener == null) return;
        for (IPeerDiscoveryListener existing : mRegisteredListeners) {
            if (existing.asBinder().equals(listener.asBinder())) return;
        }
        mRegisteredListeners.add(listener);
    }

    void unregisterDiscoveryListener(IPeerDiscoveryListener listener) {
        if (listener == null) return;
        mRegisteredListeners.removeIf(existing -> existing.asBinder().equals(listener.asBinder()));
    }

    /**
     * Snapshots this device's own capability info to advertise in discovery ACKs. hostAddress
     * is left blank -- the requester derives it from the UDP packet's actual source address
     * instead, which is more trustworthy than a self-reported value (e.g. behind NAT /
     * multi-homed devices).
     */
    private PeerInfo buildLocalPeerInfo() {
        long availableMemoryBytes = 0L;
        ActivityManager activityManager = svc.getSystemService(ActivityManager.class);
        if (activityManager != null) {
            ActivityManager.MemoryInfo memoryInfo = new ActivityManager.MemoryInfo();
            activityManager.getMemoryInfo(memoryInfo);
            availableMemoryBytes = memoryInfo.availMem;
        }

        int batteryLevelPercent = -1;
        BatteryManager batteryManager = svc.getSystemService(BatteryManager.class);
        if (batteryManager != null) {
            batteryLevelPercent = batteryManager.getIntProperty(BatteryManager.BATTERY_PROPERTY_CAPACITY);
        }

        return new PeerInfo(
                svc.getLocalSerialNumber(),
                "",                 // hostAddress -- filled in by the requester from the packet source
                GrpcTransportEngine.TRANSFER_PORT,
                LOCAL_NODE_ROLE,
                availableMemoryBytes,
                false,              // hasNpuAccelerator -- TODO: no portable on-device NPU check yet
                batteryLevelPercent,
                0L,                 // latencyMs -- measured by the requester, not the responder
                true,
                false);             // isReady -- per-capability, set later via onPeerReadyAckReceive()
    }

    /** Comma-separated wire encoding of the subset of PeerInfo an ACK actually carries. */
    private static String encodePeerInfo(PeerInfo info) {
        return info.port + "," + info.nodeRole + "," + info.availableMemoryBytes + ","
                + info.hasNpuAccelerator + "," + info.batteryLevelPercent;
    }

    /**
     * Rebuilds the responding peer's PeerInfo from the comma-separated fields appended to its
     * ACK payload, using the UDP packet's actual source address for hostAddress and marking
     * isReachable true since it just responded. Falls back to placeholder defaults if the ACK
     * is malformed or came from an older client that doesn't encode this yet.
     */
    private PeerInfo decodePeerInfo(String serialNumber, InetAddress address, String encoded) {
        try {
            String[] parts = encoded.split(",", -1);
            int port = Integer.parseInt(parts[0]);
            int nodeRole = Integer.parseInt(parts[1]);
            long availableMemoryBytes = Long.parseLong(parts[2]);
            boolean hasNpuAccelerator = Boolean.parseBoolean(parts[3]);
            int batteryLevelPercent = Integer.parseInt(parts[4]);
            return new PeerInfo(serialNumber, address != null ? address.getHostAddress() : "",
                    port, nodeRole, availableMemoryBytes, hasNpuAccelerator, batteryLevelPercent,
                    0L, true, false);
        } catch (RuntimeException e) {
            EventLog.w(TAG, "Malformed PeerInfo in ACK from " + serialNumber + "; using placeholder", e);
            return buildPlaceholderPeerInfo(serialNumber, address);
        }
    }

    /**
     * Fallback used only when an ACK's PeerInfo can't be decoded (malformed or from an older
     * client). Carries just peerId + hostAddress; the richer fields are unknown defaults.
     */
    static PeerInfo buildPlaceholderPeerInfo(String serialNumber, InetAddress address) {
        return buildPlaceholderPeerInfo(serialNumber, address != null ? address.getHostAddress() : "");
    }

    /**
     * Same placeholder as above, for a peer reached over gRPC (ready check) rather than UDP
     * discovery -- there's no packet to derive the address from, so the sender's own
     * self-reported host address (see getLocalHostAddress()/sendReadyCheck()) is used instead.
     */
    static PeerInfo buildPlaceholderPeerInfo(String serialNumber, String hostAddress) {
        return new PeerInfo(
                serialNumber,
                hostAddress != null ? hostAddress : "",
                GrpcTransportEngine.TRANSFER_PORT,
                0,      // nodeRole -- unknown
                0L,     // availableMemoryBytes -- unknown
                false,  // hasNpuAccelerator -- unknown
                -1,     // batteryLevelPercent -- unknown
                0L,     // latencyMs -- not measured yet
                true,   // isReachable -- just contacted us
                false); // isReady -- not yet confirmed ready for any capability
    }

    static void safeOnDiscoveryFailed(IPeerDiscoveryListener listener, String capability,
                                       int errorCode, String errorMessage) {
        if (listener == null) return;
        try {
            listener.onDiscoveryFailed(capability, errorCode, errorMessage);
        } catch (RemoteException e) {
            EventLog.w(TAG, "Remote listener failure in onDiscoveryFailed", e);
        }
    }

    static void safeOnPeerJoined(IPeerDiscoveryListener listener, PeerInfo peer) {
        if (listener == null) return;
        try {
            listener.onPeerJoined(peer);
        } catch (RemoteException e) {
            EventLog.w(TAG, "Remote listener failure in onPeerJoined", e);
        }
    }

    static void safeOnPeerReady(IPeerDiscoveryListener listener, String peerId) {
        if (listener == null) return;
        try {
            listener.onPeerReady(peerId);
        } catch (RemoteException e) {
            EventLog.w(TAG, "Remote listener failure in onPeerReady", e);
        }
    }

    static void safeOnPeerLost(IPeerDiscoveryListener listener, String peerId) {
        if (listener == null) return;
        try {
            listener.onPeerLost(peerId);
            EventLog.w(TAG, "onPeerLost :" + peerId);
        } catch (RemoteException e) {
            EventLog.w(TAG, "Remote listener failure in onPeerLost", e);
        }
    }

    private static void safeOnDiscoveryComplete(IPeerDiscoveryListener listener, String capability,
                                                 List<PeerInfo> peers) {
        if (listener == null) return;
        try {
            listener.onDiscoveryComplete(capability, peers);
        } catch (RemoteException e) {
            EventLog.w(TAG, "Remote listener failure in onDiscoveryComplete", e);
        }
    }

    private NetworkInterface getNetworkInterfaceForNetwork(Network network) {
        if (svc.mConnectivityManager != null && network != null) {
            LinkProperties lp = svc.mConnectivityManager.getLinkProperties(network);
            if (lp != null && lp.getInterfaceName() != null) {
                try {
                    return NetworkInterface.getByName(lp.getInterfaceName());
                } catch (SocketException e) {
                    EventLog.w(TAG, "Could not resolve NetworkInterface: " + lp.getInterfaceName(), e);
                }
            }
        }
        try {
            return NetworkInterface.getByName("wlan0");
        } catch (SocketException ignored) {
            return null;
        }
    }

    LinkAddress getWifiLinkAddress() {
        Network network = svc.mWifiNetwork;
        if (network == null || svc.mConnectivityManager == null) return null;
        LinkProperties linkProperties = svc.mConnectivityManager.getLinkProperties(network);
        if (linkProperties == null) return null;
        for (LinkAddress linkAddress : linkProperties.getLinkAddresses()) {
            if (linkAddress.getAddress() instanceof Inet4Address) {
                return linkAddress;
            }
        }
        return null;
    }

    /**
     * This device's own reachable IPv4 address, self-reported for a peer to dial back on (e.g.
     * sendReadyAck() replying to whoever sent the ready check). Unlike PeerInfo.hostAddress from
     * discovery -- which the requester derives from the actual UDP packet source, deliberately
     * not trusting a self-reported value -- there's no equivalent "packet source" available here,
     * since gRPC ready checks arrive on their own inbound connection, not one this device dials
     * out on.
     */
    String getLocalHostAddress() {
        LinkAddress linkAddress = getWifiLinkAddress();
        return linkAddress != null ? linkAddress.getAddress().getHostAddress() : "";
    }

    static final class PendingRequest {
        final String capability;
        final IPeerDiscoveryListener listener;
        final ConcurrentHashMap<String, PeerInfo> respondingPeers = new ConcurrentHashMap<>();

        PendingRequest(String capability, IPeerDiscoveryListener listener) {
            this.capability = capability;
            this.listener = listener;
        }
    }
}
