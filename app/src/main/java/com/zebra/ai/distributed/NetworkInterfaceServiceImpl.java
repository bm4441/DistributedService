package com.zebra.ai.distributed;


import java.util.List;

/**
 * Binder stub for INetworkInterfaceService, returned from DistributedService.onBind().
 * Structure only for now -- each method just logs and returns a no-op default. The actual
 * functionality (peer discovery, task dispatch, etc., calling into DistributedService's
 * internals via mService) lands in a later change.
 */
final class NetworkInterfaceServiceImpl extends INetworkInterfaceService.Stub {

    private static final String TAG = "DistributedService";

    private final DistributedService mService;

    NetworkInterfaceServiceImpl(DistributedService service) {
        mService = service;
    }

    @Override
    public void startPeerDiscovery(String capability, IPeerDiscoveryListener listener) {
        EventLog.d(TAG, "startPeerDiscovery(capability=" + capability + ")");
        mService.requestPeerDiscovery(capability, listener);
    }

    @Override
    public void stopPeerDiscovery(String capability) {
        EventLog.d(TAG, "stopPeerDiscovery(capability=" + capability + ")");
        mService.stopPeerDiscovery(capability);
    }

    @Override
    public int getPeerCount() {
        int count = mService.getKnownPeers().size();
        EventLog.d(TAG, "getPeerCount() -> " + count);
        return count;
    }

    @Override
    public List<PeerInfo> getConnectedPeers() {
        List<PeerInfo> peers = mService.getKnownPeers();
        EventLog.d(TAG, "getConnectedPeers() -> " + peers.size() + " peer(s)");
        // The peers above are returned as they are known now; if the last peer discovery is too old a new one starts, and
        // its answers reach the application through its discovery listener.
        mService.refreshPeersIfExpired();
        return mService.getKnownPeers();
    }

    @Override
    public void getReadyPeers(List<PeerInfo> peers, String capability) {
        EventLog.d(TAG, "getReadyPeers(capability=" + capability + ", " + peers.size() + " peer(s))");
        mService.getReadyPeers(peers, capability);
    }

    @Override
    public void registerDiscoveryListener(IPeerDiscoveryListener listener) {
        EventLog.d(TAG, "registerDiscoveryListener()");
        mService.registerDiscoveryListener(listener);
    }

    @Override
    public void unregisterDiscoveryListener(IPeerDiscoveryListener listener) {
        EventLog.d(TAG, "unregisterDiscoveryListener()");
        mService.unregisterDiscoveryListener(listener);
    }

    @Override
    public void submitDistinctTasks(List<DistinctTaskEnvelope> distinctTaskList,
                                     ITaskResponseCallback callback) {
        EventLog.d(TAG, "submitDistinctTasks(" + distinctTaskList.size() + " task(s))");
        mService.submitDistinctTasks(distinctTaskList, callback);
    }

    @Override
    public void submitTaskToPeer(String targetPeerId, DistinctTaskEnvelope task,
                                  ITaskResponseCallback callback) {
        EventLog.d(TAG, "submitTaskToPeer() not yet implemented");
    }

    @Override
    public void sendResponseBackToRoot(DistinctTaskEnvelope resultEnvelope) {
        EventLog.d(TAG, "sendResponseBackToRoot() not yet implemented");
    }

    @Override
    public void cancelTaskGroup(String rootGroupId) {
        EventLog.d(TAG, "cancelTaskGroup(rootGroupId=" + rootGroupId + ")");
        mService.cancelTaskGroup(rootGroupId);
    }

    @Override
    public String getLocalNodeId() {
        String nodeId = mService.getLocalSerialNumber();
        // Info, not Debug: the dashboard server finds this device (the sender) by matching this
        // exact text (server.py SENDER_RE), and the default Event Level (Info) would otherwise
        // never send it.
        EventLog.i(TAG, "getLocalNodeId() -> " + nodeId);
        return nodeId;
    }
}
