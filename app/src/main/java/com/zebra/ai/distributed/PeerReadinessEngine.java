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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    PeerReadinessEngine(DistributedService svc) {
        this.svc = svc;
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
        // Removed before it's disposed, and disposed under its own lock: an in-progress handle()
        // finishes first, and a task still waiting for it is dropped (see processDistinctTask()).
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

        Network network = svc.mWifiNetwork;
        if (network == null) {
            EventLog.w(TAG, "Cannot resume discoverability for capability '" + capability + "': Wi-Fi is not connected");
            return;
        }
        EventLog.i(TAG, "Resuming discoverability for capability '" + capability + "'");
        svc.mDiscoveryEngine.startMulticastListener(network);
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
        for (PeerInfo peer : peers) {
            if (peer.peerId == null) {
                EventLog.w(TAG, "Skipping a peer with no peerId in getReadyPeers(capability=" + capability + ")");
                continue;
            }
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

            StreamObserver<TransferResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(TransferResponse value) {
                    success[0] = value.getSuccess();
                }

                @Override
                public void onError(Throwable t) {
                    EventLog.e(TAG, "gRPC ready-check stream failed to " + peer.peerId, t);
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

            svc.mGrpcEngine.awaitReady(readyAwareObserver);
            requestObserver.onNext(FileChunk.newBuilder().setMetadata(meta).build());

            svc.mGrpcEngine.awaitReady(readyAwareObserver);
            requestObserver.onNext(FileChunk.newBuilder()
                    .setChunk(ByteString.copyFrom(payload))
                    .build());

            requestObserver.onCompleted();
            boolean completedInTime = latch.await(GrpcTransportEngine.GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            boolean finalSuccess = completedInTime && success[0];

            if (finalSuccess) {
                EventLog.i(TAG, "Ready check for capability '" + capability + "' delivered to " + peer.peerId);
            } else {
                EventLog.e(TAG, "Ready check for capability '" + capability + "' to " + peer.peerId + " failed or timed out");
            }
        } catch (Exception e) {
            EventLog.e(TAG, "Exception sending ready check to " + peer.peerId, e);
        }
    }

    /**
     * Replies to whoever sent a ready check (see sendReadyCheck()) once this device's
     * LenderHandler for that capability is warm -- dials back the sender's self-reported
     * host address, since this device may not otherwise have the sender in mKnownDevices (that
     * map only holds peers this device has itself discovered, not peers who've discovered it).
     */
    private void sendReadyAck(String targetPeerId, String targetHostAddress, String capability) {
        if (targetHostAddress == null || targetHostAddress.isEmpty()) {
            EventLog.e(TAG, "Cannot send ready ack to " + targetPeerId + ": no host address in the ready check");
            return;
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
                    EventLog.e(TAG, "gRPC ready-ack stream failed to " + targetPeerId, t);
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

            svc.mGrpcEngine.awaitReady(readyAwareObserver);
            requestObserver.onNext(FileChunk.newBuilder().setMetadata(meta).build());

            svc.mGrpcEngine.awaitReady(readyAwareObserver);
            requestObserver.onNext(FileChunk.newBuilder()
                    .setChunk(ByteString.copyFrom(payload))
                    .build());

            requestObserver.onCompleted();
            boolean completedInTime = latch.await(GrpcTransportEngine.GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            boolean finalSuccess = completedInTime && success[0];

            if (finalSuccess) {
                EventLog.i(TAG, "Ready ack for capability '" + capability + "' delivered to " + targetPeerId);
            } else {
                EventLog.e(TAG, "Ready ack for capability '" + capability + "' to " + targetPeerId + " failed or timed out");
            }
        } catch (Exception e) {
            EventLog.e(TAG, "Exception sending ready ack to " + targetPeerId, e);
        }
    }

    /**
     * Tells a reserved peer this device is done with it for this capability -- see
     * stopPeerDiscovery(). The peer's own handleReleaseReceived() reacts by running its own
     * stopPeerDiscovery(capability), releasing its handler and resuming discoverability.
     */
    private void sendReleaseNotice(PeerInfo peer, String capability) {
        try {
            byte[] payload = capability.getBytes(StandardCharsets.UTF_8);

            ManagedChannel channel = svc.mGrpcEngine.getOrCreateChannel(peer.hostAddress);
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
                    EventLog.e(TAG, "gRPC release-notice stream failed to " + peer.peerId, t);
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
                    .setFileName(GrpcTransportEngine.RELEASE_FILE_NAME_MARKER)
                    .setFileSize(payload.length)
                    .setFileIndex(0)
                    .setTotalFiles(1)
                    .build();

            svc.mGrpcEngine.awaitReady(readyAwareObserver);
            requestObserver.onNext(FileChunk.newBuilder().setMetadata(meta).build());

            svc.mGrpcEngine.awaitReady(readyAwareObserver);
            requestObserver.onNext(FileChunk.newBuilder()
                    .setChunk(ByteString.copyFrom(payload))
                    .build());

            requestObserver.onCompleted();
            boolean completedInTime = latch.await(GrpcTransportEngine.GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            boolean finalSuccess = completedInTime && success[0];

            if (finalSuccess) {
                EventLog.i(TAG, "Release notice for capability '" + capability + "' delivered to " + peer.peerId);
            } else {
                EventLog.e(TAG, "Release notice for capability '" + capability + "' to " + peer.peerId + " failed or timed out");
            }
        } catch (Exception e) {
            EventLog.e(TAG, "Exception sending release notice to " + peer.peerId, e);
        }
    }

    /**
     * Parses a ready-check datagram's payload ("senderPeerId|capability|senderHostAddress") and
     * dispatches it.
     */
    void handleReadyCheckReceived(byte[] data) {
        String payload = new String(data, StandardCharsets.UTF_8);
        String[] parts = payload.split("\\|", -1);
        String senderPeerId = parts.length > 0 ? parts[0] : payload;
        String capability = parts.length > 1 ? parts[1] : "";
        String senderHostAddress = parts.length > 2 ? parts[2] : "";

        // Cache the sender's address now, while it's on hand -- this device may otherwise never
        // learn it (mKnownDevices only holds peers this device discovered, not peers who
        // discovered it), and processDistinctTask() needs it later to route a result envelope
        // back to envelope.originatorNodeId.
        if (!senderHostAddress.isEmpty()) {
            svc.mKnownDevices.putIfAbsent(senderPeerId,
                    MulticastDiscoveryEngine.buildPlaceholderPeerInfo(senderPeerId, senderHostAddress));
        }

        onReadyCommandReceive(senderPeerId, capability, senderHostAddress);
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

    /** Called once a peer's readiness ACK (see sendReadyAck()) arrives for a capability. */
    private void onPeerReadyAckReceive(String peerId, String capability) {
        EventLog.i(TAG, "Peer " + peerId + " is ready for capability '" + capability + "'");
        svc.mKnownDevices.computeIfPresent(peerId, (id, info) -> info.withReadiness(true));
        mReadyPeersByCapability.computeIfAbsent(capability, c -> ConcurrentHashMap.newKeySet()).add(peerId);
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
            return;
        }
        long prepareStartUptimeMillis = SystemClock.uptimeMillis();
        try {
            boolean prepared;
            synchronized (handler) {
                prepared = handler.prepare();
            }
            long elapsedMillis = SystemClock.uptimeMillis() - prepareStartUptimeMillis;

            if (!prepared) {
                EventLog.e(TAG, "LenderHandler for capability '" + capability + "' reported prepare() failure"
                        + " after " + elapsedMillis + " ms (ready check from " + senderPeerId + ")");
                return;
            }
            EventLog.i(TAG, "LenderHandler for capability '" + capability + "' is now warm/ready after "
                    + elapsedMillis + " ms (ready check from " + senderPeerId + ")");

            // This device is now committed to senderPeerId's task -- stop advertising
            // ourselves to discovery requests from any other sender before telling this one
            // we're ready, so no other sender can discover and claim an already-spoken-for peer.
            EventLog.i(TAG, "Stopping multicast listener; this device is now reserved for " + senderPeerId);
            svc.mDiscoveryEngine.stopMulticastListener();

            sendReadyAck(senderPeerId, senderHostAddress, capability);
        } catch (Exception e) {
            long elapsedMillis = SystemClock.uptimeMillis() - prepareStartUptimeMillis;
            EventLog.e(TAG, "Failed to prepare LenderHandler for capability '" + capability
                    + "' after " + elapsedMillis + " ms", e);
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
