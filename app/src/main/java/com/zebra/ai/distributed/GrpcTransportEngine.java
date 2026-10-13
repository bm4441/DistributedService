package com.zebra.ai.distributed;

import android.net.LinkAddress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * gRPC transport plumbing shared by every other engine: the transfer server's lifecycle, the
 * per-peer ManagedChannel pool, the backpressure helper, and the single gRPC service
 * implementation that receives every incoming stream and dispatches it (by its
 * FileMetadata.file_name marker) to whichever engine actually handles that kind of payload.
 * Split out of DistributedService verbatim -- see DistributedService's own fields/methods for
 * the shared state this reaches into (mService).
 */
final class GrpcTransportEngine {

    private static final String TAG = "DistributedService";

    /* ================= gRPC & Transfer Constants ================= */
    static final int TRANSFER_PORT = 58232;
    static final int CHUNK_SIZE = 64 * 1024;
    static final int FLOW_CONTROL_WINDOW = 4 * 1024 * 1024;
    static final int MAX_MESSAGE_SIZE = 8 * 1024 * 1024;
    // The longest any send waits: for the stream to become ready again (a peer that stops reading), and for the peer's reply once a message
    // is sent. A peer that went offline is given up -- and reported lost -- after this long, so it is shown at once and not after a minute.
    static final int GRPC_TIMEOUT_SECONDS = 60;
    static final long READY_POLL_INTERVAL_MILLIS = 5;
    // Sentinel FileMetadata.file_name value marking a "ready check" datagram (see
    // PeerReadinessEngine.getReadyPeers()/sendReadyCheck()) rather than a marshalled
    // DistinctTaskEnvelope -- both now travel over the same TransferFiles RPC, so onCompleted()
    // needs a way to tell them apart before deciding how to decode the buffered bytes.
    static final String READY_CHECK_FILE_NAME_MARKER = "__READY_CHECK__";
    // Sentinel for the reply a peer sends back once its LenderHandler for the requested
    // capability has finished warming up -- see PeerReadinessEngine.prepareLenderHandler()/
    // sendReadyAck().
    static final String READY_ACK_FILE_NAME_MARKER = "__READY_ACK__";
    // Sentinel for the notice a sender pushes to a peer it had reserved, telling it to release
    // that capability's handler and resume discoverability -- see PeerReadinessEngine.
    // stopPeerDiscovery()/sendReleaseNotice().
    static final String RELEASE_FILE_NAME_MARKER = "__RELEASE__";
    // Sentinel for the notice a contributor sends the requestor that holds it when it becomes a Requestor itself: it takes
    // no more tasks -- see PeerReadinessEngine.leaveContributorRole()/handleWithdrawReceived().
    static final String WITHDRAW_FILE_NAME_MARKER = "__WITHDRAW__";
    // Sentinel for the sign of life a requestor sends every few seconds to each contributor it holds: a contributor that
    // hears nothing from the requestor it is reserved for lets it go -- see PeerReadinessEngine.sendHoldBeats()/handleHoldReceived().
    static final String HOLD_FILE_NAME_MARKER = "__HOLD__";

    final ConcurrentHashMap<String, ManagedChannel> mChannelPool = new ConcurrentHashMap<>();
    private final Object mTransferServerLock = new Object();
    private volatile io.grpc.Server mGrpcServer;

    private final DistributedService svc;

    GrpcTransportEngine(DistributedService svc) {
        this.svc = svc;
    }

    boolean isTransferServerActive() {
        synchronized (mTransferServerLock) {
            return mGrpcServer != null && !mGrpcServer.isShutdown() && !mGrpcServer.isTerminated();
        }
    }

    void startTransferServer() {
        synchronized (mTransferServerLock) {
            if (!svc.isServiceActive()) {
                EventLog.i(TAG, "Not starting the gRPC transfer server: the service is paused by its policy");
                return;
            }
            if (isTransferServerActive()) {
                return;
            }
            stopTransferServer();

            LinkAddress linkAddress = svc.mDiscoveryEngine.getWifiLinkAddress();
            if (linkAddress == null) {
                EventLog.e(TAG, "No Wi-Fi IPv4 address found; gRPC server not started.");
                return;
            }

            try {
                // Bind gRPC OkHttp Server to Wi-Fi IPv4 and TRANSFER_PORT
                mGrpcServer = io.grpc.okhttp.OkHttpServerBuilder
                        .forPort(TRANSFER_PORT, io.grpc.InsecureServerCredentials.create())
                        .flowControlWindow(FLOW_CONTROL_WINDOW)
                        .maxInboundMessageSize(MAX_MESSAGE_SIZE)
                        .keepAliveTime(20, TimeUnit.SECONDS)
                        .keepAliveTimeout(5, TimeUnit.SECONDS)
                        .permitKeepAliveWithoutCalls(true)
                        .permitKeepAliveTime(15, TimeUnit.SECONDS)
                        .addService(new FileTransferServiceImpl())
                        .build()
                        .start();

                EventLog.i(TAG, "gRPC Transfer Server listening on port " + TRANSFER_PORT);
            } catch (IOException e) {
                EventLog.e(TAG, "Failed to start gRPC server on port " + TRANSFER_PORT, e);
            }
        }
    }

    void stopTransferServer() {
        synchronized (mTransferServerLock) {
            if (mGrpcServer != null) {
                try {
                    mGrpcServer.shutdown();
                    EventLog.i(TAG, "gRPC Transfer Server stopped.");
                } catch (Exception e) {
                    EventLog.w(TAG, "Error shutting down gRPC server", e);
                } finally {
                    mGrpcServer = null;
                }
            }
        }
    }

    /**
     * Logs a gRPC call that failed. A peer that cannot be reached -- its Wi-Fi is off, its service is stopped (the
     * connection is refused), it does not answer in time -- is an expected event, which is handled by reporting the peer lost
     * (see PeerReadinessEngine.tellPeerLost()): it is one warning line with the reason, without the stack trace. Any other
     * failure is unexpected and is logged as an error, with its stack trace.
     */
    static void logCallFailure(String what, Throwable t) {
        Status status = Status.fromThrowable(t);
        Status.Code code = status.getCode();
        if (code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED) {
            Throwable cause = status.getCause();
            String detail = cause != null && cause.getMessage() != null ? cause.getMessage() : status.getDescription();
            EventLog.w(TAG, what + ": " + code + (detail != null ? " (" + detail + ")" : ""));
        } else {
            EventLog.e(TAG, what, t);
        }
    }

    /**
     * The state of the connection to this address, asking it to connect if it is idle -- null if there is no channel to it
     * (none was made, or it was shut down). Never creates one. Used by PeerLossWatcher.
     */
    io.grpc.ConnectivityState connectionState(String ipAddress) {
        ManagedChannel channel = mChannelPool.get(ipAddress);
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            return null;
        }
        return channel.getState(true);
    }

    /** Closes every connection to a peer -- the service is paused by its policy. New ones are made when needed. */
    void closeAllChannels() {
        for (String ip : new java.util.ArrayList<>(mChannelPool.keySet())) {
            ManagedChannel channel = mChannelPool.remove(ip);
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    ManagedChannel getOrCreateChannel(String ipAddress) {
        return mChannelPool.compute(ipAddress, (ip, existingChannel) -> {
            if (existingChannel != null && !existingChannel.isShutdown() && !existingChannel.isTerminated()) {
                return existingChannel;
            }
            return io.grpc.okhttp.OkHttpChannelBuilder
                    .forAddress(ip, TRANSFER_PORT)
                    .flowControlWindow(FLOW_CONTROL_WINDOW)
                    .maxInboundMessageSize(MAX_MESSAGE_SIZE)
                    .keepAliveTime(20, TimeUnit.SECONDS)
                    .keepAliveTimeout(5, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .usePlaintext()
                    .build();
        });
    }

    /**
     * Blocks the calling (background executor) thread until the transport is ready to accept
     * another message, instead of calling onNext() unconditionally. Without this, a fast
     * local source can queue unbounded data client-side ahead of what the HTTP/2
     * flow-control window / TCP socket can actually drain.
     */
    void awaitReady(ClientCallStreamObserver<FileChunk> observer) {
        if (observer.isReady()) return;
        long waitStartNanos = System.nanoTime();
        while (!observer.isReady()) {
            try {
                Thread.sleep(READY_POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        long waitedMillis = (System.nanoTime() - waitStartNanos) / 1_000_000;
        EventLog.d(TAG, "gRPC backpressure: waited " + waitedMillis + " ms for transport readiness");
    }

    /**
     * Bounded variant of awaitReady() for uploads that run one at a time on a dedicated thread --
     * task uploads on each peer's own transfer thread (see PeerTransferWorker) and results on the
     * result-transfer thread (see ResultTransferEngine.queueResult()) -- where an upload stuck
     * here would stall everything queued behind it, not just itself. Returns false instead of
     * waiting forever once the call has already ended (callDone counted down: a failed call, e.g.
     * the peer dropped off, reports isReady() == false for good) or after maxWaitMillis without the
     * transport becoming ready (e.g. a peer that stopped reading). True means ready to send.
     */
    boolean awaitReady(ClientCallStreamObserver<FileChunk> observer, CountDownLatch callDone,
                       long maxWaitMillis) {
        if (observer.isReady()) return true;
        long waitStartNanos = System.nanoTime();
        long giveUpAtNanos = waitStartNanos + TimeUnit.MILLISECONDS.toNanos(maxWaitMillis);
        while (!observer.isReady()) {
            if (callDone.getCount() == 0 || System.nanoTime() >= giveUpAtNanos) {
                EventLog.w(TAG, "gRPC backpressure: gave up after " + (System.nanoTime() - waitStartNanos) / 1_000_000
                        + " ms waiting for transport readiness"
                        + (callDone.getCount() == 0 ? " (call ended)" : " (timed out)"));
                return false;
            }
            try {
                Thread.sleep(READY_POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        long waitedMillis = (System.nanoTime() - waitStartNanos) / 1_000_000;
        EventLog.d(TAG, "gRPC backpressure: waited " + waitedMillis + " ms for transport readiness");
        return true;
    }

    /** "4521334 bytes (4.52 MB)" -- exact byte count plus decimal megabytes (1 MB = 10^6 bytes). */
    static String formatSize(long bytes) {
        return bytes + " bytes (" + String.format(Locale.US, "%.2f", bytes / 1_000_000.0) + " MB)";
    }

    /**
     * "1.76 MB/s (14.1 Mbit/s)" for bytes moved in millis, in the decimal units network
     * throughput is usually quoted in (1 MB = 10^6 bytes, 1 Mbit = 10^6 bits).
     */
    static String formatThroughput(long bytes, long millis) {
        if (millis <= 0) {
            return "n/a (under 1 ms)";
        }
        double bytesPerSecond = bytes * 1000.0 / millis;
        return String.format(Locale.US, "%.2f MB/s (%.1f Mbit/s)",
                bytesPerSecond / 1_000_000.0, bytesPerSecond * 8 / 1_000_000.0);
    }

    /**
     * gRPC Service Implementation: receives a stream of FileChunk messages carrying one
     * Parcel-marshalled DistinctTaskEnvelope (see TaskTransferEngine.submitDistinctTasks/
     * sendDistinctTask), or one of the ready-check/ready-ack/release-notice sentinel payloads.
     * The envelope's own fields travel intact inside the marshalled bytes -- FileMetadata is
     * only used for a human-readable log line (task id / position in the batch) and for the
     * sentinel-vs-envelope dispatch below; it is not used to reconstruct the envelope.

     * NOTE: this repurposes the same TransferFiles RPC the old file-attachment engine
     * (performAttachmentTransfer, since removed) used for raw file bytes. If that old path is
     * ever reactivated, it would no longer work against this receiver, since every incoming
     * stream is now assumed to be a marshalled DistinctTaskEnvelope or a sentinel payload.
     */
    private final class FileTransferServiceImpl extends FileTransferServiceGrpc.FileTransferServiceImplBase {
        @Override
        public StreamObserver<FileChunk> transferFiles(StreamObserver<TransferResponse> responseObserver) {
            return new StreamObserver<>() {
                private String currentSender = "UNKNOWN";
                private String currentTaskId = "UNKNOWN";
                private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                private int receivedChunkCount = 0;
                // When this stream's metadata arrived -- the start of the receive-side transfer
                // time logged in onCompleted(). 0 until then.
                private long streamStartNanos = 0;

                @Override
                public void onNext(FileChunk value) {
                    if (value.hasMetadata()) {
                        FileMetadata meta = value.getMetadata();
                        currentSender = meta.getSenderSerial();
                        currentTaskId = meta.getFileName();
                        buffer.reset();
                        receivedChunkCount = 0;
                        streamStartNanos = System.nanoTime();
                        // A sign of life comes every few seconds, from every requestor that holds this device: not worth a line.
                        if (!HOLD_FILE_NAME_MARKER.equals(currentTaskId)) {
                            EventLog.i(TAG, "gRPC receiving task envelope [" + (meta.getFileIndex() + 1) + "/"
                                    + meta.getTotalFiles() + "] taskId=" + currentTaskId + " from " + currentSender
                                    + " (" + meta.getFileSize() + " bytes)");
                        }

                    } else if (value.hasChunk()) {
                        byte[] chunkBytes = value.getChunk().toByteArray();
                        buffer.write(chunkBytes, 0, chunkBytes.length);
                        receivedChunkCount++;
                        // The busiest log line (one per 64 KB chunk): with Log Print off and the
                        // Event Level above Debug, skip even building the message.
                        if (EventLog.isDebugEnabled()) {
                            EventLog.d(TAG, "gRPC received chunk #" + receivedChunkCount + " ("
                                    + chunkBytes.length + " bytes) for taskId=" + currentTaskId);
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    // CANCELLED is the sender giving up on this upload -- e.g. its transfer thread
                    // being stopped during cleanup -- so it's expected and needs no stack trace.
                    if (Status.fromThrowable(t).getCode() == Status.Code.CANCELLED) {
                        EventLog.w(TAG, "gRPC stream for taskId=" + currentTaskId + " from " + currentSender
                                + " was cancelled by the sender");
                    } else {
                        logCallFailure("gRPC task envelope stream error (taskId=" + currentTaskId + ")", t);
                    }
                }

                @Override
                public void onCompleted() {
                    if (READY_CHECK_FILE_NAME_MARKER.equals(currentTaskId)) {
                        // null = taken up; otherwise why this device cannot be ready for the sender, which the
                        // sender reads in the reply and reports this device lost (see PeerReadinessEngine.tellPeerLost()).
                        String refusal = svc.mReadinessEngine.handleReadyCheckReceived(buffer.toByteArray());
                        responseObserver.onNext(TransferResponse.newBuilder()
                                .setSuccess(refusal == null)
                                .setMessage(refusal == null ? "Ready check received" : refusal)
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }

                    if (READY_ACK_FILE_NAME_MARKER.equals(currentTaskId)) {
                        svc.mReadinessEngine.handleReadyAckReceived(buffer.toByteArray());
                        responseObserver.onNext(TransferResponse.newBuilder()
                                .setSuccess(true)
                                .setMessage("Ready ack received")
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }

                    if (WITHDRAW_FILE_NAME_MARKER.equals(currentTaskId)) {
                        svc.mReadinessEngine.handleWithdrawReceived(currentSender, buffer.toByteArray());
                        responseObserver.onNext(TransferResponse.newBuilder()
                                .setSuccess(true)
                                .setMessage("Withdraw notice received")
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }

                    if (HOLD_FILE_NAME_MARKER.equals(currentTaskId)) {
                        // null = this device is reserved for the sender; otherwise it is not (any more), which the sender
                        // reads in the reply and reports this device lost.
                        String refusal = svc.mReadinessEngine.handleHoldReceived(currentSender);
                        responseObserver.onNext(TransferResponse.newBuilder()
                                .setSuccess(refusal == null)
                                .setMessage(refusal == null ? "Hold received" : refusal)
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }

                    if (RELEASE_FILE_NAME_MARKER.equals(currentTaskId)) {
                        svc.mReadinessEngine.handleReleaseReceived(buffer.toByteArray());
                        responseObserver.onNext(TransferResponse.newBuilder()
                                .setSuccess(true)
                                .setMessage("Release notice received")
                                .build());
                        responseObserver.onCompleted();
                        return;
                    }

                    // The receiving end's view of the transfer the sender times: from this stream's
                    // metadata arriving to its last byte, so it leaves out connection setup and the
                    // reply's trip back -- compare with the sender's "transfer time"/"throughput".
                    if (streamStartNanos != 0) {
                        long receiveMillis = (System.nanoTime() - streamStartNanos) / 1_000_000;
                        EventLog.i(TAG, "gRPC received taskId=" + currentTaskId + " from " + currentSender
                                + ": payload=" + formatSize(buffer.size()) + " in " + receiveMillis
                                + "ms, throughput=" + formatThroughput(buffer.size(), receiveMillis));
                    }

                    // A task from the requestor this device is reserved for is as good a sign of life as a hold.
                    svc.mReadinessEngine.noteHolderActivity(currentSender);
                    DistinctTaskEnvelope envelope = TaskTransferEngine.unmarshallEnvelope(buffer.toByteArray());
                    boolean success = envelope != null;
                    String refusal = null;
                    if (success) {
                        // Prints once per received envelope, per the current "just confirm the
                        // transfer works" scope. Real per-task handling happens in
                        // onDataReceived() once that's implemented.
                        EventLog.i(TAG, "gRPC task data received from " + currentSender
                                + " (taskId=" + envelope.taskId + ")");
                        // null = taken; otherwise why this device cannot process the task, which the sender reads in
                        // the reply and reports this device lost (see TaskTransferEngine.sendDistinctTask()).
                        refusal = svc.mTaskEngine.onDataReceived(envelope);
                        success = refusal == null;
                    } else {
                        EventLog.e(TAG, "Failed to reconstruct DistinctTaskEnvelope from received bytes "
                                + "(taskId=" + currentTaskId + ")");
                    }
                    responseObserver.onNext(TransferResponse.newBuilder()
                            .setSuccess(success)
                            .setMessage(refusal != null ? refusal : success ? "Received" : "Failed to decode envelope")
                            .build());
                    responseObserver.onCompleted();
                }
            };
        }
    }
}
