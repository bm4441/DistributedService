package com.zebra.ai.distributed;

import android.content.Context;
import android.net.Network;
import android.os.SystemClock;

import com.zebra.ai.vision.detector.BarcodeDecoder;
import com.zebra.ai.vision.detector.DistributedModuleRecognizer;
import com.zebra.ai.vision.detector.DistributedTextOCR;
import com.zebra.ai.vision.detector.EntityType;
import com.zebra.ai.vision.detector.InferencerOptions;
import com.zebra.ai.vision.detector.ModuleRecognizer;
import com.zebra.ai.vision.detector.TextOCR;
import com.zebra.ai.vision.internal.distributed.LenderHandler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * "Keep peer ready" protocol: the ready-check / ready-ack / release-notice wire messages, the
 * per-capability LenderHandler warm-up/release lifecycle, and the bookkeeping of which peers
 * this device has reserved as their sender. Split out of DistributedService verbatim -- see
 * DistributedService's own fields/methods for the shared state this reaches into (mService).
 */
final class PeerReadinessEngine {

    private static final String TAG = "DistributedService";

    // How long stopPeerDiscovery() waits for the per-peer transfer threads to finish after
    // stopping them, before sending release notices. They normally stop within milliseconds.
    private static final long PEER_WORKER_STOP_WAIT_MILLIS = 2000L;
    private static final String BARCODE_DECODER_MODEL_NAME = "barcode-decoder";
    private static final String OCR_MODEL_NAME = "text-ocr-recognizer";

    // Peers that have sent this device a ready-ack for a capability (i.e. peers this device has
    // reserved as their sender) -- see onPeerReadyAckReceive()/stopPeerDiscovery().
    private final ConcurrentHashMap<String, Set<String>> mReadyPeersByCapability = new ConcurrentHashMap<>();

    /** What a contributor's reply to a ready check starts with when it cannot be ready for this requestor. */
    static final String CANT_BE_READY_PREFIX = "Can't be ready: ";
    // How long a requestor waits for the ready ack after its ready check was delivered: the contributor loads its model
    // first, which takes seconds. Without it the contributor is reported lost.
    private static final long READY_ACK_TIMEOUT_MILLIS = 30_000L;
    // When the service becomes a Requestor while it is a Contributor at work (leaveContributorRole()): how long it gives
    // the tasks it already accepted to be completed, and then the results of those to be sent back, before the two threads
    // are released regardless. Frames take about a second each.
    private static final long FINISH_TASKS_MAX_WAIT_MILLIS = 30_000L;
    private static final long FINISH_RESULTS_MAX_WAIT_MILLIS = 15_000L;

    // Requestor side. Peers reported lost to the application (onPeerLost) and not heard from since -- each is reported once
    // -- and the "peerId|capability" of every ready check whose ready ack is still awaited.
    private final Set<String> mLostPeers = ConcurrentHashMap.newKeySet();
    private final Set<String> mAwaitingReadyAck = ConcurrentHashMap.newKeySet();

    // The requestor this device is reserved for -- the one whose ready check it took up first -- or null while it is free. A
    // Contributor takes up one requestor at a time: a ready check that arrives while it is reserved is dropped (see
    // handleReadyCheckReceived()). Freed by releaseReservation(), when the requestor releases it or the ready check failed.
    private final AtomicReference<String> mReservedFor = new AtomicReference<>();

    // A requestor that holds contributors sends each one a sign of life (a "hold", see sendHoldBeats()) every
    // HOLD_BEAT_PERIOD_MILLIS. A contributor that hears nothing from the requestor it is reserved for -- no hold, no task,
    // no repeated ready check -- for HOLDER_SILENCE_LIMIT_MILLIS lets it go (see watchHolder()): that requestor was switched
    // off, lost its Wi-Fi or restarted, and its release notice will never come.
    private static final long HOLD_BEAT_PERIOD_MILLIS = 3_000L;
    private static final long HOLDER_SILENCE_LIMIT_MILLIS = 20_000L;
    // Contributor side. When the requestor this device is reserved for was last heard from (SystemClock.uptimeMillis(): it
    // does not run while the device sleeps, so a nap of this device is not counted as the requestor's silence), and the
    // check that is due to look at it. Written by the gRPC threads and the scheduler, hence the lock for the future.
    private volatile long mHolderHeardAtMillis;
    private final Object mHolderWatchLock = new Object();
    private ScheduledFuture<?> mHolderWatch;
    private String mWatchedHolder;                     // the requestor mHolderWatch is about
    // Requestor side. The periodic sending of the holds (null while this device holds nobody), and the peers a hold is on its
    // way to right now -- one at a time each, so a peer that answers slowly does not collect a pile of them.
    private final Object mHoldBeatsLock = new Object();
    private ScheduledFuture<?> mHoldBeats;
    private final Set<String> mHoldsInFlight = ConcurrentHashMap.newKeySet();

    // Lazily built, one long-lived LenderHandler per capability -- see getOrCreateLenderHandler().
    final Map<String, LenderHandler> mLenderHandlers = new ConcurrentHashMap<>();

    /**
     * Registry of known capabilities this device can lend. Keyed by the same capability string
     * a ready check/task envelope carries. Adding a capability is just adding an entry here.
     */
    private static final Map<String, LenderHandlerFactory> LENDER_HANDLER_FACTORIES;

    static {
        Map<String, LenderHandlerFactory> factories = new HashMap<>();

        // Factory for OCR (assuming it doesn't need context, otherwise wrap it)
        factories.put("ocr.recognize", (context) -> DistributedTextOCR.createLenderHandler());

        // Optimized Factory for Product Frame
        LenderHandlerFactory productFrameFactory = (context) -> {
            try {
                // Note: Ensure your asset files are named without spaces (e.g., product_1.index)
                // for better compatibility. I've used the names from your original code.
                File indexFile = copyAssetToFiles(context, "product.index");
                File labelFile = copyAssetToFiles(context, "product.txt");

                ModuleRecognizer.Settings settings = new ModuleRecognizer.Settings("product-and-shelf-recognizer");
                settings.enableProductRecognitionWithIndex(
                        "product-and-shelf-recognizer",
                        indexFile.getAbsolutePath(),
                        labelFile.getAbsolutePath());

                return DistributedModuleRecognizer.createLenderHandler(settings);
            }  catch (Exception e) {
                EventLog.e(TAG, "Failed to create LenderHandler factory for product.frame", e);
                throw e;
            }
        };
        factories.put("product.frame", productFrameFactory);

        LenderHandlerFactory subtaskLenderFactory = (context) -> {
            try {
                // Settings creation is identical to the full product frame handler
                File indexFile = copyAssetToFiles(context, "product.index");
                File labelFile = copyAssetToFiles(context, "product.txt");

                ModuleRecognizer.Settings settings = new ModuleRecognizer.Settings("product-and-shelf-recognizer");
                settings.enableProductRecognitionWithIndex(
                        "product-and-shelf-recognizer",
                        indexFile.getAbsolutePath(),
                        labelFile.getAbsolutePath());
                Integer[] rpo = { InferencerOptions.DSP, InferencerOptions.CPU, InferencerOptions.GPU };
                BarcodeDecoder.Settings barcodeDecoderSettings =
                        new BarcodeDecoder.Settings(BARCODE_DECODER_MODEL_NAME);
                barcodeDecoderSettings.detectorSetting.inferencerOptions.defaultDims.width = 640;
                barcodeDecoderSettings.detectorSetting.inferencerOptions.defaultDims.height = 640;
                barcodeDecoderSettings.detectorSetting.inferencerOptions.runtimeProcessorOrder = rpo;
                barcodeDecoderSettings.enableAIBarcodeDecode = true;
                Map<EntityType, BarcodeDecoder.Settings> barcodeSettings = new HashMap<>();
                barcodeSettings.put(EntityType.LABEL, barcodeDecoderSettings);
                settings.enableBarcodeRecognition(barcodeSettings);
                settings.enableTextOCR(new TextOCR.Settings(OCR_MODEL_NAME));

                return DistributedModuleRecognizer.createSubtaskLenderHandler(settings);
            } catch (Exception e) {
                EventLog.e(TAG, "Failed to create LenderHandler factory for product.embed_lookup", e);
                throw e;
            }
        };
        factories.put("product.embed_lookup", subtaskLenderFactory);

        LENDER_HANDLER_FACTORIES = Map.copyOf(factories);
    }

    private final DistributedService svc;

    // Requestor side: notices a contributor whose connection drops while nothing is being sent to it.
    private final PeerLossWatcher mLossWatcher;

    PeerReadinessEngine(DistributedService svc) {
        this.svc = svc;
        mLossWatcher = new PeerLossWatcher(new PeerLossWatcher.Host() {
            @Override
            public io.grpc.ConnectivityState connectionState(String hostAddress) {
                return PeerReadinessEngine.this.svc.mGrpcEngine.connectionState(hostAddress);
            }

            @Override
            public boolean isWifiUp() {
                return PeerReadinessEngine.this.svc.mWifiNetwork != null;
            }

            @Override
            public void peerLost(String peerId, String why) {
                tellPeerLost(peerId, why);
            }
        }, svc.mScheduler, SystemClock::elapsedRealtime);
    }

    private static File copyAssetToFiles(Context context, String assetName) throws IOException {
        File outFile = new File(context.getFilesDir(), assetName);
        if (outFile.exists() && outFile.length() > 0) {
            return outFile; // Skip if already exists and is not empty
        }

        // Write to a temporary file first to ensure atomicity
        File tmpFile = new File(outFile.getPath() + ".tmp");
        try (InputStream in = context.getAssets().open(assetName);
             OutputStream out = new FileOutputStream(tmpFile)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
        }

        // Rename the temporary file to the final destination file
        if (!tmpFile.renameTo(outFile)) {
            throw new IOException("Failed to rename temporary file to " + outFile.getName());
        }
        return outFile;
    }

    /**
     * Entry point for NetworkInterfaceServiceImpl.stopPeerDiscovery() -- the local/AIDL-driven
     * side of releasing a capability. Releases this device's own reservation (see
     * releaseCapability()), then pushes a release notice (see sendReleaseNotice()) to every peer
     * that had sent *this* device a ready-ack for the capability -- i.e. peers this device had
     * reserved as their sender. Each one reacts by releasing its own reservation too (see
     * handleReleaseReceived()) -- otherwise a peer that answered a ready check but never received
     * an actual task would stay reserved (and undiscoverable) forever, since nothing else tells
     * it the sender has moved on.

     * Deliberately NOT reused by handleReleaseReceived(): that path must only release *this*
     * device's own reservation, not fan out further release notices of its own -- a device that
     * has also reserved peers of its own for this capability (acting as a sender elsewhere) would
     * otherwise incorrectly release those unrelated reservations just because it was released by
     * someone else. releaseCapability() is the shared part; the peer fan-out below is not.
     */
    void stopPeerDiscovery(String capability) {
        // Stop this device's per-peer transfer threads first -- dropping any tasks still waiting
        // on them and cancelling any upload in progress -- and wait for them to finish, so no
        // task is still being streamed to a peer when its release notice (below) goes out. The
        // next getReadyPeers() starts fresh ones.
        svc.mTaskEngine.shutdownPeerWorkers(PEER_WORKER_STOP_WAIT_MILLIS);

        releaseCapability(capability);

        // This requestor's session is over: nothing is awaited from any peer, and none counts as lost any more.
        mAwaitingReadyAck.removeIf(key -> key.endsWith("|" + capability));
        mLostPeers.clear();
        mLossWatcher.clear();

        Set<String> readyPeerIds = mReadyPeersByCapability.remove(capability);
        if (readyPeerIds != null) {
            for (String peerId : readyPeerIds) {
                PeerInfo peer = svc.mKnownDevices.get(peerId);
                if (peer != null) {
                    svc.mTransferExecutor.execute(() -> sendReleaseNotice(peer, capability));
                } else {
                    EventLog.w(TAG, "Cannot send release notice to " + peerId + ": no longer in mKnownDevices");
                }
            }
        }
    }

    /**
     * The service is paused by its policy (see DistributedService.pauseEngines()): as a requestor this device stops asking, so
     * the contributors it holds are released -- they would stay reserved for a requestor that no longer comes -- and what it
     * knew about its session (who was ready, who was lost, which ready acks it waited for) is forgotten. Blocks while the
     * notices go out (each is bounded), so not on the main thread.
     */
    void releaseHeldPeersAndForget() {
        for (Map.Entry<String, Set<String>> entry : new ArrayList<>(mReadyPeersByCapability.entrySet())) {
            for (String peerId : new ArrayList<>(entry.getValue())) {
                PeerInfo peer = svc.mKnownDevices.get(peerId);
                if (peer != null) {
                    sendReleaseNotice(peer, entry.getKey());
                }
            }
        }
        mReadyPeersByCapability.clear();
        mAwaitingReadyAck.clear();
        mLostPeers.clear();
        mLossWatcher.clear();
    }

    /**
     * Releases this device's own reservation for a capability: disposes its LenderHandler (and
     * whatever warm model it's holding, e.g. LenderModelSlot) -- it's rebuilt and re-warmed from
     * scratch by the next ready check (see getOrCreateLenderHandler()/prepareLenderHandler()) --
     * and resumes discoverability by rejoining the multicast group. Despite stopPeerDiscovery()'s
     * name, this doesn't stop anything -- it re-enables it: prepareLenderHandler() calls
     * stopMulticastListener() once this device commits to a sender's task, so no other sender can
     * discover an already-spoken-for peer; this is how it signals it's available again. Safe to
     * call even if already listening -- startMulticastListener() no-ops in that case.
     */
    private void releaseCapability(String capability) {
        releaseReservation();

        disposeLenderHandler(capability);

        Network network = svc.mWifiNetwork;
        if (network == null) {
            EventLog.w(TAG, "Cannot resume discoverability for capability '" + capability + "': Wi-Fi is not connected");
            return;
        }
        EventLog.i(TAG, "Resuming discoverability for capability '" + capability + "'");
        svc.mDiscoveryEngine.startMulticastListener(network);
    }

    /**
     * Removes this capability's LenderHandler and disposes it (freeing whatever warm model it holds). Removed before it's
     * disposed, and disposed under its own lock: an in-progress handle() finishes first, and a task still waiting for it is
     * dropped (see processDistinctTask()).
     */
    private void disposeLenderHandler(String capability) {
        LenderHandler handler = mLenderHandlers.remove(capability);
        if (handler != null) {
            synchronized (handler) {
                try {
                    handler.dispose();
                    EventLog.i(TAG, "Disposed LenderHandler for capability '" + capability + "'");
                } catch (Exception e) {
                    EventLog.w(TAG, "Error disposing LenderHandler for capability '" + capability + "'", e);
                }
            }
        }
    }

    /**
     * Not yet wired up to any AIDL method -- caller/wiring is a later step. For each peer in the
     * given list, sends a lightweight "ready check" datagram over the same gRPC transport engine
     * sendDistinctTask() uses (per-peer channel, FileChunk streaming, backpressure, keepalive),
     * carrying this device's own peerId and the capability being checked. The receiving peer's
     * onReadyCommandReceive() is invoked once it arrives -- currently just a log, real
     * readiness-reporting logic (Ready / Partially Ready / Failed) is a later step too.
     * Before sending, each peer also gets its own transfer thread (see
     * TaskTransferEngine.ensurePeerWorker()), which will carry the distinct tasks sent to it.
     */
    void getReadyPeers(java.util.List<PeerInfo> peers, String capability) {
        // Each peer once: the list can hold the same peer more than once (PeerInfo has no equals(), so its peerId is what
        // tells), and it must not be sent a second ready check. The first one stays and the order is kept; a peer with no
        // peerId is left for the loop below to skip.
        Set<String> seenPeerIds = new HashSet<>();
        peers.removeIf(peer -> peer.peerId != null && !seenPeerIds.add(peer.peerId));

        if (!svc.isServiceActive()) {
            // Paused by the service policy: nobody is asked, and the application is told at once that these peers are out
            // of reach (it will ask again once peers are discovered again), instead of waiting for ready acks.
            for (PeerInfo peer : peers) {
                tellPeerLost(peer.peerId, "this device's service is paused by its policy");
            }
            return;
        }

        for (PeerInfo peer : peers) {
            if (peer.peerId == null) {
                EventLog.w(TAG, "Skipping a peer with no peerId in getReadyPeers(capability=" + capability + ")");
                continue;
            }
            // A new attempt with this peer: if it fails again, the application is told again.
            mLostPeers.remove(peer.peerId);
            svc.mTaskEngine.ensurePeerWorker(peer.peerId);
            svc.mTransferExecutor.execute(() -> sendReadyCheck(peer, capability));
        }
    }

    private void sendReadyCheck(PeerInfo peer, String capability) {
        try {
            // Carries this device's own host address so the peer can dial back a readiness ACK
            // (see sendReadyAck()) once its handler finishes warming up -- the peer has no other
            // way to reach this device unless it happens to already be in its own mKnownDevices.
            byte[] payload = (svc.getLocalSerialNumber() + "|" + capability + "|" + svc.mDiscoveryEngine.getLocalHostAddress())
                    .getBytes(StandardCharsets.UTF_8);

            ManagedChannel channel = svc.mGrpcEngine.getOrCreateChannel(peer.hostAddress);
            FileTransferServiceGrpc.FileTransferServiceStub asyncStub =
                    FileTransferServiceGrpc.newStub(channel);

            CountDownLatch latch = new CountDownLatch(1);
            final boolean[] success = new boolean[]{false};
            // What the peer said in its reply (TransferResponse.message); and whether the call itself failed.
            final String[] reply = new String[]{""};
            final boolean[] callFailed = new boolean[]{false};

            StreamObserver<TransferResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(TransferResponse value) {
                    success[0] = value.getSuccess();
                    reply[0] = value.getMessage();
                }

                @Override
                public void onError(Throwable t) {
                    GrpcTransportEngine.logCallFailure("gRPC ready-check stream failed to " + peer.peerId, t);
                    callFailed[0] = true;
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };

            StreamObserver<FileChunk> requestObserver = asyncStub.transferFiles(responseObserver);
            ClientCallStreamObserver<FileChunk> readyAwareObserver =
                    (ClientCallStreamObserver<FileChunk>) requestObserver;

            FileMetadata meta = FileMetadata.newBuilder()
                    .setSenderSerial(svc.getLocalSerialNumber())
                    .setFileName(GrpcTransportEngine.READY_CHECK_FILE_NAME_MARKER)
                    .setFileSize(payload.length)
                    .setFileIndex(0)
                    .setTotalFiles(1)
                    .build();

            // Bounded waits (see GrpcTransportEngine.awaitReady(observer, callDone, maxWaitMillis)): towards a peer that is
            // disconnected the call fails at once and never becomes ready, so waiting without a bound would never end -- and
            // the peer could never be reported lost.
            long maxStallMillis = TimeUnit.SECONDS.toMillis(GrpcTransportEngine.GRPC_TIMEOUT_SECONDS);
            boolean streamed = svc.mGrpcEngine.awaitReady(readyAwareObserver, latch, maxStallMillis);
            if (streamed) {
                requestObserver.onNext(FileChunk.newBuilder().setMetadata(meta).build());
                streamed = svc.mGrpcEngine.awaitReady(readyAwareObserver, latch, maxStallMillis);
            }
            if (streamed) {
                requestObserver.onNext(FileChunk.newBuilder()
                        .setChunk(ByteString.copyFrom(payload))
                        .build());
            }

            boolean completedInTime = false;
            if (streamed) {
                requestObserver.onCompleted();
                completedInTime = latch.await(GrpcTransportEngine.GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } else {
                readyAwareObserver.cancel("Ready check to " + peer.peerId + " could not be sent", null);
            }
            boolean finalSuccess = completedInTime && success[0];

            if (finalSuccess) {
                EventLog.i(TAG, "Ready check for capability '" + capability + "' delivered to " + peer.peerId);
                watchForReadyAck(peer.peerId, capability);
                mLossWatcher.watch(peer.peerId, peer.hostAddress);
            } else if (completedInTime && !callFailed[0] && reply[0].startsWith(CANT_BE_READY_PREFIX)) {
                // The peer answered, and said it will not be ready for this device.
                EventLog.w(TAG, "Ready check for capability '" + capability + "' refused by " + peer.peerId + ": " + reply[0]);
                tellPeerLost(peer.peerId, reply[0]);
            } else {
                EventLog.e(TAG, "Ready check for capability '" + capability + "' to " + peer.peerId + " failed or timed out");
                tellPeerLost(peer.peerId, "it did not answer the ready check (disconnected or not responding)");
            }
        } catch (Exception e) {
            EventLog.e(TAG, "Exception sending ready check to " + peer.peerId, e);
            tellPeerLost(peer.peerId, "the ready check could not be sent to it: " + e);
        }
    }

    /**
     * After a ready check was delivered, waits for the peer's ready ack: if it has not come within READY_ACK_TIMEOUT_MILLIS
     * the peer is reported lost. A peer that is ready already (this ready check was a repeat) is not waited for.
     */
    private void watchForReadyAck(String peerId, String capability) {
        Set<String> ready = mReadyPeersByCapability.get(capability);
        if (ready != null && ready.contains(peerId)) {
            return;
        }
        String key = peerId + "|" + capability;
        if (!mAwaitingReadyAck.add(key)) {
            return;                                    // already watching: an earlier ready check of the same session
        }
        ensureHoldBeats();
        try {
            svc.mScheduler.schedule(() -> {
                if (mAwaitingReadyAck.remove(key)) {
                    tellPeerLost(peerId, "no ready ack came within " + READY_ACK_TIMEOUT_MILLIS / 1000
                            + " s of the ready check for capability '" + capability + "'");
                }
            }, READY_ACK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            mAwaitingReadyAck.remove(key);             // the service is shutting down
        }
    }

    /**
     * A peer answered a discovery request: it is (back) among the peers, so it no longer counts as lost -- if it fails
     * again the application is told again (see tellPeerLost()). Called just before the application is told it joined.
     */
    void peerJoined(String peerId) {
        if (peerId == null || mLostPeers.isEmpty()) {
            return;                                    // no peer is lost: nothing to do
        }
        if (mLostPeers.contains(peerId)) {
            mLostPeers.remove(peerId);
            EventLog.i(TAG, "Peer " + peerId + ", reported lost before, joined again");
        }
        // Not among the lost peers: nothing to do, the join carries on.
    }

    /**
     * Reports a contributor lost to the application: calls onPeerLost() on the registered discovery listeners, once -- the
     * peer is not reported again until this device asks it again (getReadyPeers()), or it answers. The application then
     * stops using the peer, and the SDK looks for another. Called when the peer refuses or does not answer a ready check,
     * when its ready ack does not come, and when it refuses or does not answer a task (see TaskTransferEngine.sendDistinctTask()).
     */
    void tellPeerLost(String peerId, String why) {
        if (peerId == null) {
            return;
        }
        mLossWatcher.unwatch(peerId);                  // reported (or about to be): the watcher has nothing more to say about it
        if (!mLostPeers.add(peerId)) {
            return;
        }
        EventLog.w(TAG, "Peer " + peerId + " is lost: " + why);
        for (IPeerDiscoveryListener listener : svc.mDiscoveryEngine.mRegisteredListeners) {
            MulticastDiscoveryEngine.safeOnPeerLost(listener, peerId);
        }
    }

    /**
     * Replies to whoever sent a ready check (see sendReadyCheck()) once this device's
     * LenderHandler for that capability is warm -- dials back the sender's self-reported
     * host address, since this device may not otherwise have the sender in mKnownDevices (that
     * map only holds peers this device has itself discovered, not peers who've discovered it).
     */
    private boolean sendReadyAck(String targetPeerId, String targetHostAddress, String capability) {
        if (targetHostAddress == null || targetHostAddress.isEmpty()) {
            EventLog.e(TAG, "Cannot send ready ack to " + targetPeerId + ": no host address in the ready check");
            return false;
        }
        // Start the receiver's processing and result-transfer threads now, before telling the
        // sender this device is ready, so both are already running when its tasks arrive.
        svc.mTaskEngine.startReceiverThreads();
        try {
            byte[] payload = (svc.getLocalSerialNumber() + "|" + capability).getBytes(StandardCharsets.UTF_8);

            ManagedChannel channel = svc.mGrpcEngine.getOrCreateChannel(targetHostAddress);
            FileTransferServiceGrpc.FileTransferServiceStub asyncStub =
                    FileTransferServiceGrpc.newStub(channel);

            CountDownLatch latch = new CountDownLatch(1);
            final boolean[] success = new boolean[]{false};

            StreamObserver<TransferResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(TransferResponse value) {
                    success[0] = value.getSuccess();
                }

                @Override
                public void onError(Throwable t) {
                    GrpcTransportEngine.logCallFailure("gRPC ready-ack stream failed to " + targetPeerId, t);
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };

            StreamObserver<FileChunk> requestObserver = asyncStub.transferFiles(responseObserver);
            ClientCallStreamObserver<FileChunk> readyAwareObserver =
                    (ClientCallStreamObserver<FileChunk>) requestObserver;

            FileMetadata meta = FileMetadata.newBuilder()
                    .setSenderSerial(svc.getLocalSerialNumber())
                    .setFileName(GrpcTransportEngine.READY_ACK_FILE_NAME_MARKER)
                    .setFileSize(payload.length)
                    .setFileIndex(0)
                    .setTotalFiles(1)
                    .build();

            // Bounded waits, as in sendReadyCheck(): towards a peer that is gone the call fails at once and never becomes
            // ready, so an unbounded wait would never end.
            boolean completedInTime = sendMarkedPayload(readyAwareObserver, requestObserver, latch, meta, payload,
                    "Ready ack to " + targetPeerId);
            boolean finalSuccess = completedInTime && success[0];

            if (finalSuccess) {
                EventLog.i(TAG, "Ready ack for capability '" + capability + "' delivered to " + targetPeerId);
            } else {
                EventLog.e(TAG, "Ready ack for capability '" + capability + "' to " + targetPeerId + " failed or timed out");
            }
            return finalSuccess;
        } catch (Exception e) {
            EventLog.e(TAG, "Exception sending ready ack to " + targetPeerId, e);
            return false;
        }
    }

    /**
     * Sends the one small message of a ready ack or a release notice -- metadata, then the payload -- on a call that is
     * already open, and waits for the peer's reply. Every wait is bounded: when the peer is disconnected the call has
     * already failed (callDone) and is given up at once; one that stopped reading is given up after GRPC_TIMEOUT_SECONDS.
     * Returns true if the call completed (the reply is in the caller's observer); false if it was given up, in which case
     * it is cancelled.
     */
    private boolean sendMarkedPayload(ClientCallStreamObserver<FileChunk> call, StreamObserver<FileChunk> requestObserver,
                                      CountDownLatch callDone, FileMetadata meta, byte[] payload, String what)
            throws InterruptedException {
        long maxStallMillis = TimeUnit.SECONDS.toMillis(GrpcTransportEngine.GRPC_TIMEOUT_SECONDS);
        boolean streamed = svc.mGrpcEngine.awaitReady(call, callDone, maxStallMillis);
        if (streamed) {
            requestObserver.onNext(FileChunk.newBuilder().setMetadata(meta).build());
            streamed = svc.mGrpcEngine.awaitReady(call, callDone, maxStallMillis);
        }
        if (streamed) {
            requestObserver.onNext(FileChunk.newBuilder().setChunk(ByteString.copyFrom(payload)).build());
            requestObserver.onCompleted();
            return callDone.await(GrpcTransportEngine.GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        call.cancel(what + " could not be sent", null);
        return false;
    }

    /**
     * Tells a reserved peer this device is done with it for this capability -- see
     * stopPeerDiscovery(). The peer's own handleReleaseReceived() reacts by running its own
     * stopPeerDiscovery(capability), releasing its handler and resuming discoverability.
     */
    private void sendReleaseNotice(PeerInfo peer, String capability) {
        boolean delivered = sendMarkedMessage(peer.hostAddress, peer.peerId, GrpcTransportEngine.RELEASE_FILE_NAME_MARKER,
                capability.getBytes(StandardCharsets.UTF_8), "release notice");
        if (delivered) {
            EventLog.i(TAG, "Release notice for capability '" + capability + "' delivered to " + peer.peerId);
        } else {
            EventLog.e(TAG, "Release notice for capability '" + capability + "' to " + peer.peerId + " failed or timed out");
        }
    }

    /**
     * Sends one small marked message (a release or a withdraw notice) to a peer over gRPC, with every wait bounded, and
     * returns whether the peer received it.
     */
    private boolean sendMarkedMessage(String hostAddress, String peerId, String marker, byte[] payload, String what) {
        TransferResponse reply = exchangeMarkedMessage(hostAddress, peerId, marker, payload, what);
        return reply != null && reply.getSuccess();
    }

    /**
     * Sends one small marked message as sendMarkedMessage() does, and returns the peer's reply -- whether it took the message
     * (getSuccess()) and what it said (getMessage()) -- or null if the call failed or was given up.
     */
    private TransferResponse exchangeMarkedMessage(String hostAddress, String peerId, String marker, byte[] payload,
                                                   String what) {
        try {
            ManagedChannel channel = svc.mGrpcEngine.getOrCreateChannel(hostAddress);
            FileTransferServiceGrpc.FileTransferServiceStub asyncStub =
                    FileTransferServiceGrpc.newStub(channel);

            CountDownLatch latch = new CountDownLatch(1);
            final TransferResponse[] reply = new TransferResponse[]{null};

            StreamObserver<TransferResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(TransferResponse value) {
                    reply[0] = value;
                }

                @Override
                public void onError(Throwable t) {
                    GrpcTransportEngine.logCallFailure("gRPC " + what + " stream failed to " + peerId, t);
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };

            StreamObserver<FileChunk> requestObserver = asyncStub.transferFiles(responseObserver);
            ClientCallStreamObserver<FileChunk> readyAwareObserver =
                    (ClientCallStreamObserver<FileChunk>) requestObserver;

            FileMetadata meta = FileMetadata.newBuilder()
                    .setSenderSerial(svc.getLocalSerialNumber())
                    .setFileName(marker)
                    .setFileSize(payload.length)
                    .setFileIndex(0)
                    .setTotalFiles(1)
                    .build();

            boolean completedInTime = sendMarkedPayload(readyAwareObserver, requestObserver, latch, meta, payload,
                    what + " to " + peerId);
            return completedInTime ? reply[0] : null;
        } catch (Exception e) {
            EventLog.e(TAG, "Exception sending " + what + " to " + peerId, e);
            return null;
        }
    }

    /**
     * Requestor side: makes sure the sign of life (a "hold") goes out every HOLD_BEAT_PERIOD_MILLIS to each contributor this
     * device holds -- one that was sent a ready check (its ready ack awaited) or has answered it (ready). Starts when the
     * first one is held; ends by itself when none is.
     */
    private void ensureHoldBeats() {
        synchronized (mHoldBeatsLock) {
            if (mHoldBeats != null) {
                return;
            }
            try {
                mHoldBeats = svc.mScheduler.scheduleWithFixedDelay(this::sendHoldBeats, HOLD_BEAT_PERIOD_MILLIS,
                        HOLD_BEAT_PERIOD_MILLIS, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // the service is shutting down
            }
        }
    }

    /** The contributors this device holds, as peerId -> capability. */
    private Map<String, String> heldPeers() {
        Map<String, String> held = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : mReadyPeersByCapability.entrySet()) {
            for (String peerId : entry.getValue()) {
                held.put(peerId, entry.getKey());
            }
        }
        for (String key : mAwaitingReadyAck) {
            int bar = key.indexOf('|');
            if (bar > 0) {
                held.putIfAbsent(key.substring(0, bar), key.substring(bar + 1));
            }
        }
        return held;
    }

    /** One round of holds, run on the scheduler: each contributor that is held and not reported lost is sent one. */
    private void sendHoldBeats() {
        Map<String, String> held;
        synchronized (mHoldBeatsLock) {
            held = heldPeers();
            if (held.isEmpty() && mHoldBeats != null) {
                mHoldBeats.cancel(false);              // nobody is held: started again by the next one
                mHoldBeats = null;
                return;
            }
        }
        if (!svc.isServiceActive() || svc.mWifiNetwork == null) {
            return;                                    // paused, or out of Wi-Fi: nobody can be reached
        }
        for (Map.Entry<String, String> entry : held.entrySet()) {
            String peerId = entry.getKey();
            String capability = entry.getValue();
            PeerInfo peer = svc.mKnownDevices.get(peerId);
            if (peer == null || peer.hostAddress == null || mLostPeers.contains(peerId) || !mHoldsInFlight.add(peerId)) {
                continue;                              // unknown, already reported lost, or its last hold is still on its way
            }
            try {
                svc.mTransferExecutor.execute(() -> {
                    try {
                        sendHold(peer, capability);
                    } finally {
                        mHoldsInFlight.remove(peerId);
                    }
                });
            } catch (RejectedExecutionException e) {
                mHoldsInFlight.remove(peerId);         // the service is shutting down
            }
        }
    }

    /**
     * Sends one hold. A contributor that cannot be reached is not reported here (the connection watcher does that, see
     * PeerLossWatcher); one that answers it is not reserved for this device -- it let this device go after too long a
     * silence, or never held it -- is reported lost, so that the application stops using it.
     */
    private void sendHold(PeerInfo peer, String capability) {
        TransferResponse reply = exchangeMarkedMessage(peer.hostAddress, peer.peerId, GrpcTransportEngine.HOLD_FILE_NAME_MARKER,
                capability.getBytes(StandardCharsets.UTF_8), "hold");
        if (reply == null) {
            EventLog.d(TAG, "The hold to " + peer.peerId + " did not get through");
            return;
        }
        if (!reply.getSuccess() && reply.getMessage().startsWith(CANT_BE_READY_PREFIX)) {
            EventLog.w(TAG, "Peer " + peer.peerId + " answered the hold: " + reply.getMessage());
            tellPeerLost(peer.peerId, reply.getMessage());
        }
    }

    /**
     * The service has just become a Requestor -- an application bound to it -- while it was a Contributor. From now on it
     * asks for work and lends none, so what it was doing as a Contributor is wound down (see windDownContributorWork()).
     * Blocks while it waits, so it runs off the main thread (see DistributedService.applicationBound()).
     */
    void leaveContributorRole() {
        windDownContributorWork("The service became a Requestor", "the service is now a Requestor");
    }

    /**
     * The service is paused by its policy (see DistributedService.pauseEngines()): what it was doing as a Contributor is
     * wound down the same way. Blocks while it waits, so not on the main thread.
     */
    void pauseContributorWork() {
        windDownContributorWork("The service is paused by its policy", "the service is paused by its policy");
    }

    /**
     * Winds down a Contributor's work, in this order:
     *  1. New tasks are refused (TaskTransferEngine.onDataReceived(): the service is paused or not a Contributor any more),
     *     and the requestor that holds this device is told it is gone: a withdraw notice (sendWithdrawNotice()), which that
     *     requestor turns into onPeerLost().
     *  2. The tasks already accepted -- the one being processed and those queued behind it -- are completed, and the
     *     processing thread is released (TaskTransferEngine.finishProcessingThread()).
     *  3. Their results are sent back to the originator, and the result-transfer thread is released
     *     (ResultTransferEngine.finishResultTransferThread()).
     *  4. The models are freed and the reservation ends.
     * If the service is working as a Contributor again by the time the waiting is over, the models and the reservation are
     * left alone: they may belong to a new requestor by then.
     */
    private void windDownContributorWork(String why, String reasonForRequestor) {
        String holder = mReservedFor.get();
        EventLog.i(TAG, why + ": it takes no more tasks"
                + (holder != null ? ", and tells the requestor that holds it (" + holder + ") that it is withdrawn" : ""));
        if (holder != null) {
            svc.mTransferExecutor.execute(() -> sendWithdrawNotice(holder, reasonForRequestor));
        }

        svc.mTaskEngine.finishProcessingThread(FINISH_TASKS_MAX_WAIT_MILLIS);
        svc.mResultEngine.finishResultTransferThread(FINISH_RESULTS_MAX_WAIT_MILLIS);

        boolean contributorAgain = svc.getServiceMode() == ServiceMode.CONTRIBUTOR && svc.isServiceActive();
        if (!contributorAgain) {
            for (String capability : new ArrayList<>(mLenderHandlers.keySet())) {
                disposeLenderHandler(capability);
            }
        }
        if (holder != null) {
            releaseReservationFor(holder);
        }
        EventLog.i(TAG, "The contributor work is wound down: the processing and result-transfer threads are released");
    }

    /**
     * Tells the requestor that holds this device that it is withdrawn -- it is a Requestor now and takes no tasks. The
     * requestor reads it (handleWithdrawReceived()) and reports this device lost to its application. Best effort, bounded:
     * a requestor that is not reached learns the same from the next task it sends, which is refused.
     */
    private void sendWithdrawNotice(String requestorPeerId, String reason) {
        PeerInfo requestor = svc.mKnownDevices.get(requestorPeerId);
        if (requestor == null || requestor.hostAddress == null || requestor.hostAddress.isEmpty()) {
            EventLog.w(TAG, "Cannot send the withdraw notice to " + requestorPeerId + ": its address is not known");
            return;
        }
        byte[] payload = (CANT_BE_READY_PREFIX + reason).getBytes(StandardCharsets.UTF_8);
        boolean delivered = sendMarkedMessage(requestor.hostAddress, requestorPeerId,
                GrpcTransportEngine.WITHDRAW_FILE_NAME_MARKER, payload, "withdraw notice");
        if (delivered) {
            EventLog.i(TAG, "Withdraw notice delivered to " + requestorPeerId);
        } else {
            EventLog.w(TAG, "Withdraw notice to " + requestorPeerId + " failed or timed out");
        }
    }

    /**
     * A contributor this device holds sent a withdraw notice: it became a Requestor and will take no tasks. The
     * application is told it is lost (tellPeerLost()) and stops using it.
     */
    void handleWithdrawReceived(String senderPeerId, byte[] data) {
        String words = new String(data, StandardCharsets.UTF_8);
        EventLog.i(TAG, "Withdraw notice received from " + senderPeerId + ": " + words);
        tellPeerLost(senderPeerId, "it withdrew: " + words);
    }

    /**
     * Parses a ready-check datagram's payload ("senderPeerId|capability|senderHostAddress") and
     * dispatches it -- if this device can take it up:
     *  - It must be a Contributor (see ServiceMode): a Requestor asks for work and lends none, so the ready check is dropped.
     *  - It must not be reserved already. The first ready check that arrives reserves this device for its sender, and the
     *    handler is warmed up and the ready ack sent for it. While this device is reserved, every other ready check is
     *    dropped -- those of other requestors, and repeats from the one it is reserved for. The reservation ends when that
     *    requestor releases the device (see releaseReservation()).
     * Returns null when the ready check was taken up (or is a repeat from the requestor this device is reserved for), and
     * otherwise the words to answer the requestor with -- "Can't be ready: ..." -- so that it knows this device will not be
     * ready for it and reports it lost (see tellPeerLost()).
     */
    String handleReadyCheckReceived(byte[] data) {
        String payload = new String(data, StandardCharsets.UTF_8);
        String[] parts = payload.split("\\|", -1);
        String senderPeerId = parts.length > 0 ? parts[0] : payload;
        String capability = parts.length > 1 ? parts[1] : "";
        String senderHostAddress = parts.length > 2 ? parts[2] : "";

        if (!svc.isServiceActive()) {
            EventLog.i(TAG, "Dropping the ready check from " + senderPeerId + " for capability '" + capability
                    + "': the service is paused by its policy");
            return CANT_BE_READY_PREFIX + "the service is paused by its policy";
        }
        ServiceMode mode = svc.getServiceMode();
        if (mode != ServiceMode.CONTRIBUTOR) {
            EventLog.i(TAG, "Dropping the ready check from " + senderPeerId + " for capability '" + capability
                    + "': the service is in " + mode.label + " mode, not Contributor");
            return CANT_BE_READY_PREFIX + "the service is in " + mode.label + " mode, not Contributor";
        }
        if (!mReservedFor.compareAndSet(null, senderPeerId)) {
            String reservedFor = mReservedFor.get();
            EventLog.i(TAG, "Dropping the ready check from " + senderPeerId + " for capability '" + capability
                    + "': this device is already reserved for " + (reservedFor != null ? reservedFor : "another requestor"));
            // A repeat from the requestor this device is reserved for is dropped without a word: the requestor has what it
            // asked for already (the SDK asks again while it waits). Any other requestor is told it cannot have this device.
            if (senderPeerId.equals(reservedFor)) {
                noteHolderActivity(senderPeerId);
                return null;
            }
            return CANT_BE_READY_PREFIX + "this device is already reserved for another requestor";
        }
        EventLog.i(TAG, "This device is now reserved for " + senderPeerId + " (capability '" + capability + "')");
        watchHolder(senderPeerId);

        // Cache the sender's address now, while it's on hand -- this device may otherwise never
        // learn it (mKnownDevices only holds peers this device discovered, not peers who
        // discovered it), and processDistinctTask() needs it later to route a result envelope
        // back to envelope.originatorNodeId.
        if (!senderHostAddress.isEmpty()) {
            svc.mKnownDevices.putIfAbsent(senderPeerId,
                    MulticastDiscoveryEngine.buildPlaceholderPeerInfo(senderPeerId, senderHostAddress));
        }

        onReadyCommandReceive(senderPeerId, capability, senderHostAddress);
        return null;
    }

    /** The requestor this device is reserved for, or null while it is free. */
    String getReservedFor() {
        return mReservedFor.get();
    }

    /** Parses a ready-ack datagram's payload ("peerId|capability") and dispatches it. */
    void handleReadyAckReceived(byte[] data) {
        String payload = new String(data, StandardCharsets.UTF_8);
        int pipeIndex = payload.indexOf('|');
        String peerId = pipeIndex >= 0 ? payload.substring(0, pipeIndex) : payload;
        String capability = pipeIndex >= 0 ? payload.substring(pipeIndex + 1) : "";
        onPeerReadyAckReceive(peerId, capability);
    }

    /**
     * Parses a release notice's payload (just the capability -- see sendReleaseNotice()) and
     * reacts to it by releasing this device's own reservation (see releaseCapability()). Calls
     * releaseCapability() directly, NOT stopPeerDiscovery() -- this device didn't decide to stop
     * anything, it was just told the sender that reserved it has moved on, so it must not also
     * fan out release notices to any peers *it* has independently reserved for this capability.
     */
    void handleReleaseReceived(byte[] data) {
        String capability = new String(data, StandardCharsets.UTF_8);
        EventLog.i(TAG, "Release notice received for capability '" + capability + "'");
        // The sender that reserved this device is done with it: stop the result-transfer thread
        // (dropping results it no longer wants) before becoming discoverable to other senders
        // again. The next ready ack starts a fresh one (TaskTransferEngine.startReceiverThreads()).
        svc.mResultEngine.shutdownResultTransferThread();
        releaseCapability(capability);
    }

    /**
     * Ends this device's reservation, if it has one: the next ready check -- from any requestor -- is taken up again. Called
     * when the requestor it is reserved for releases it (releaseCapability()), when Wi-Fi is lost (the requestor is out of
     * reach, see DistributedService), and when the ready check it was reserved for could not be completed.
     */
    void releaseReservation() {
        String reservedFor = mReservedFor.getAndSet(null);
        if (reservedFor != null) {
            stopWatchingHolder(reservedFor);
            EventLog.i(TAG, "This device is no longer reserved for " + reservedFor);
        }
    }

    /**
     * Contributor side: the requestor this device is reserved for sends a sign of life (see sendHoldBeats()). Answers null
     * when this device is reserved for the sender -- the time it was heard is noted -- and otherwise the words to tell the
     * sender with: this device is not reserved for it (any more), so the sender reports it lost.
     */
    String handleHoldReceived(String senderPeerId) {
        if (senderPeerId != null && senderPeerId.equals(mReservedFor.get())) {
            noteHolderActivity(senderPeerId);
            return null;
        }
        String reservedFor = mReservedFor.get();
        EventLog.d(TAG, "Hold from " + senderPeerId + " answered: this device is "
                + (reservedFor != null ? "reserved for " + reservedFor : "not reserved"));
        return CANT_BE_READY_PREFIX + "this device is not reserved for you"
                + (reservedFor != null ? " (it is reserved for another requestor)" : " (it let you go)");
    }

    /** Notes that the requestor this device is reserved for has just been heard from (a hold, a task, a repeated ready check). */
    void noteHolderActivity(String senderPeerId) {
        if (senderPeerId != null && senderPeerId.equals(mReservedFor.get())) {
            mHolderHeardAtMillis = SystemClock.uptimeMillis();
        }
    }

    /**
     * Starts watching the requestor this device was just reserved for: the clock starts now, and each sign of life from it
     * (noteHolderActivity()) restarts it. Ends with the reservation (stopWatchingHolder()).
     */
    private void watchHolder(String holder) {
        mHolderHeardAtMillis = SystemClock.uptimeMillis();
        scheduleHolderCheck(holder, HOLDER_SILENCE_LIMIT_MILLIS);
    }

    private void scheduleHolderCheck(String holder, long delayMillis) {
        synchronized (mHolderWatchLock) {
            if (mHolderWatch != null) {
                mHolderWatch.cancel(false);
            }
            if (!holder.equals(mReservedFor.get())) {
                mHolderWatch = null;                   // released in the meantime
                return;
            }
            try {
                mHolderWatch = svc.mScheduler.schedule(() -> checkHolder(holder), delayMillis, TimeUnit.MILLISECONDS);
                mWatchedHolder = holder;
            } catch (RejectedExecutionException e) {
                mHolderWatch = null;                   // the service is shutting down
            }
        }
    }

    /** Ends the watch on this requestor -- not one already started on a requestor that reserved this device since. */
    private void stopWatchingHolder(String holder) {
        synchronized (mHolderWatchLock) {
            if (mHolderWatch != null && holder.equals(mWatchedHolder)) {
                mHolderWatch.cancel(false);
                mHolderWatch = null;
            }
        }
    }

    /** Looks at the requestor this device is reserved for: heard from recently -- look again when its time would be up. */
    private void checkHolder(String holder) {
        if (!holder.equals(mReservedFor.get())) {
            return;                                    // released, or reserved for another requestor now (which has its own check)
        }
        long silentMillis = SystemClock.uptimeMillis() - mHolderHeardAtMillis;
        if (silentMillis < HOLDER_SILENCE_LIMIT_MILLIS) {
            scheduleHolderCheck(holder, HOLDER_SILENCE_LIMIT_MILLIS - silentMillis);
            return;
        }
        // Off the scheduler thread: freeing a model waits for the task that is using it.
        try {
            svc.mTransferExecutor.execute(() -> letHolderGo(holder, silentMillis));
        } catch (RejectedExecutionException e) {
            // the service is shutting down
        }
    }

    /**
     * The requestor this device is reserved for has sent nothing for HOLDER_SILENCE_LIMIT_MILLIS: it is switched off, out of
     * Wi-Fi range or restarted, and will not release this device. Does what a release notice from it would (see
     * handleReleaseReceived()), for every capability: the result-transfer thread is released, dropping results nobody waits
     * for, the models are freed -- a task still waiting for its model is dropped, see processDistinctTask() -- the reservation
     * ends and the device becomes discoverable again. The reservation is ended after the rest, so that no new requestor
     * reserves this device while it is still being cleaned up.
     */
    private void letHolderGo(String holder, long silentMillis) {
        EventLog.w(TAG, "The requestor " + holder + " that this device is reserved for sent nothing for " + silentMillis / 1000
                + " s: letting it go");
        svc.mResultEngine.shutdownResultTransferThread();
        for (String capability : new ArrayList<>(mLenderHandlers.keySet())) {
            disposeLenderHandler(capability);
        }
        if (!mReservedFor.compareAndSet(holder, null)) {
            return;                                    // released by someone else while this was going on
        }
        stopWatchingHolder(holder);
        EventLog.i(TAG, "This device is no longer reserved for " + holder);
        Network network = svc.mWifiNetwork;
        if (network == null) {
            EventLog.w(TAG, "Cannot resume discoverability: Wi-Fi is not connected");
            return;
        }
        EventLog.i(TAG, "Resuming discoverability");
        svc.mDiscoveryEngine.startMulticastListener(network);
    }

    /** Called once a peer's readiness ACK (see sendReadyAck()) arrives for a capability. */
    private void onPeerReadyAckReceive(String peerId, String capability) {
        EventLog.i(TAG, "Peer " + peerId + " is ready for capability '" + capability + "'");
        mAwaitingReadyAck.remove(peerId + "|" + capability);
        if (mLostPeers.remove(peerId)) {
            EventLog.i(TAG, "Peer " + peerId + ", reported lost before, is ready after all");
        }
        svc.mKnownDevices.computeIfPresent(peerId, (id, info) -> info.withReadiness(true));
        mReadyPeersByCapability.computeIfAbsent(capability, c -> ConcurrentHashMap.newKeySet()).add(peerId);
        ensureHoldBeats();
        for (IPeerDiscoveryListener listener : svc.mDiscoveryEngine.mRegisteredListeners) {
            MulticastDiscoveryEngine.safeOnPeerReady(listener, peerId);
        }
    }

    /**
     * Called when a "ready check" datagram (see getReadyPeers()/sendReadyCheck()) arrives from a
     * peer. Dispatches the actual model warm-up to a background executor and returns
     * immediately -- see prepareLenderHandler() for why.
     */
    private void onReadyCommandReceive(String senderPeerId, String capability, String senderHostAddress) {
        EventLog.i(TAG, "onReadyCommandReceive: senderPeerId=" + senderPeerId + ", capability=" + capability);
        svc.mTransferExecutor.execute(() -> prepareLenderHandler(senderPeerId, capability, senderHostAddress));
    }

    /**
     * Gets or lazily builds the LenderHandler for this capability, then warms it -- per the
     * LenderHandler contract, prepare() must run once per incoming connection/session, before
     * it's ever asked to handle() a real request, so that submitDistinctTasks()'s eventual
     * arrival for this capability doesn't pay the cold-model-load cost (prepare() is blocking
     * and can take real seconds). Runs on mTransferExecutor, never on the gRPC completion
     * thread that received the ready check. Once warm, replies to the sender with a readiness
     * ACK (see sendReadyAck()) so it knows this peer can now actually handle the capability --
     * no ACK is sent on failure, since the peer isn't ready.
     */
    private void prepareLenderHandler(String senderPeerId, String capability, String senderHostAddress) {
        LenderHandler handler = getOrCreateLenderHandler(capability);
        if (handler == null) {
            EventLog.e(TAG, "No LenderHandler available for capability '" + capability
                    + "' (ready check from " + senderPeerId + ")");
            releaseReservationFor(senderPeerId);
            return;
        }
        long prepareStartUptimeMillis = SystemClock.uptimeMillis();
        try {
            boolean prepared;
            synchronized (handler) {
                svc.mInferenceMeter.begin();
                try {
                    prepared = handler.prepare();
                } finally {
                    svc.mInferenceMeter.end();
                }
            }
            long elapsedMillis = SystemClock.uptimeMillis() - prepareStartUptimeMillis;

            if (!prepared) {
                EventLog.e(TAG, "LenderHandler for capability '" + capability + "' reported prepare() failure"
                        + " after " + elapsedMillis + " ms (ready check from " + senderPeerId + ")");
                releaseReservationFor(senderPeerId);
                return;
            }
            EventLog.i(TAG, "LenderHandler for capability '" + capability + "' is now warm/ready after "
                    + elapsedMillis + " ms (ready check from " + senderPeerId + ")");

            if (svc.getServiceMode() != ServiceMode.CONTRIBUTOR || !svc.isServiceActive()) {
                // The service became a Requestor (or was paused by its policy) while the model was warming up: the requestor was already told this device
                // is withdrawn (leaveContributorRole()), so there is no ready ack, and nothing is reserved.
                EventLog.i(TAG, "The service is no longer working as a Contributor (Requestor or paused) while the handler was"
                        + " warming up: no ready ack for " + senderPeerId);
                disposeLenderHandler(capability);
                releaseReservationFor(senderPeerId);
                return;
            }

            if (!senderPeerId.equals(mReservedFor.get())) {
                // The requestor was let go while the model was warming up (it sent nothing for too long): nobody is waiting
                // for a ready ack, and the device may be reserved for another requestor by now.
                EventLog.i(TAG, "This device is no longer reserved for " + senderPeerId + " after the warm-up: no ready ack");
                if (mReservedFor.get() == null) {
                    disposeLenderHandler(capability);
                }
                return;
            }

            // This device is now committed to senderPeerId's task -- stop advertising
            // ourselves to discovery requests from any other sender before telling this one
            // we're ready, so no other sender can discover and claim an already-spoken-for peer.
            EventLog.i(TAG, "Stopping multicast listener; this device is now reserved for " + senderPeerId);
            svc.mDiscoveryEngine.stopMulticastListener();

            if (!sendReadyAck(senderPeerId, senderHostAddress, capability)) {
                // The requestor was never told this device is ready, so it will never release it: free it here --
                // this also starts the multicast listener again, which was stopped above.
                EventLog.w(TAG, "The ready ack to " + senderPeerId + " did not get through; releasing this device");
                releaseCapability(capability);
            }
        } catch (Exception e) {
            long elapsedMillis = SystemClock.uptimeMillis() - prepareStartUptimeMillis;
            EventLog.e(TAG, "Failed to prepare LenderHandler for capability '" + capability
                    + "' after " + elapsedMillis + " ms", e);
            releaseReservationFor(senderPeerId);
        }
    }

    /** Ends the reservation only if it is for this requestor: a newer one, for another requestor, is not touched. */
    private void releaseReservationFor(String senderPeerId) {
        if (mReservedFor.compareAndSet(senderPeerId, null)) {
            stopWatchingHolder(senderPeerId);
            EventLog.i(TAG, "This device is no longer reserved for " + senderPeerId);
        }
    }

    /** Builds a LenderHandler for one capability -- see LENDER_HANDLER_FACTORIES. */
    @FunctionalInterface
    private interface LenderHandlerFactory {
        LenderHandler create(Context context) throws Exception;
    }

    /**
     * Returns the cached LenderHandler for this capability, building it on first use via
     * LENDER_HANDLER_FACTORIES.
     */
    LenderHandler getOrCreateLenderHandler(String capability) {
        return mLenderHandlers.computeIfAbsent(capability, this::createLenderHandlerFor);
    }

    private LenderHandler createLenderHandlerFor(String capability) {
        LenderHandlerFactory factory = LENDER_HANDLER_FACTORIES.get(capability);
        if (factory == null) {
            EventLog.e(TAG, "No LenderHandler factory registered for capability '" + capability + "'");
            return null;
        }
        try {
            return factory.create(this.svc);
        } catch (Exception e) {
            EventLog.e(TAG, "Failed to build LenderHandler for capability '" + capability + "'", e);
            return null;
        }
    }
}
