package com.zebra.ai.distributed;

import android.os.Parcel;
import android.os.SharedMemory;
import android.os.SystemClock;

import com.google.protobuf.ByteString;
import com.zebra.ai.vision.internal.distributed.LenderHandler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.grpc.ManagedChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Data transfer: submitting/sending distinct tasks to a peer, receiving them on the other end,
 * and running the actual LenderHandler processing. Split out of DistributedService verbatim --
 * see DistributedService's own fields/methods for the shared state this reaches into (mService).
 */
final class TaskTransferEngine {

    private static final String TAG = "DistributedService";

    private final DistributedService svc;

    // Running sum/count backing the all-time average LenderHandler.handle() processing time
    // logged on every processed task -- see processDistinctTask(). Receiver-side only, so it
    // lives here rather than in ResultTransferEngine.
    private final AtomicLong mProcessingCount = new AtomicLong(0);
    private final AtomicLong mTotalProcessingMillis = new AtomicLong(0);

    // One dedicated transfer thread per peer, keyed by peerId -- created when the peer is asked to
    // get ready (see PeerReadinessEngine.getReadyPeers()) and kept for the distinct-task transfers
    // that follow. See PeerTransferWorker.
    final ConcurrentHashMap<String, PeerTransferWorker> mPeerWorkers = new ConcurrentHashMap<>();

    // The receiver's one task-processing thread: every received task is processed here, one at a
    // time in arrival order, and hands its result to ResultTransferEngine's result-transfer thread
    // (see processDistinctTask()) -- so processing the next task and sending the previous result
    // happen in parallel. Started just before this device's first ready ack (see
    // startReceiverThreads()).
    private final ThreadPoolExecutor mProcessingExecutor = new ThreadPoolExecutor(1, 1, 0L,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
            new LoggingThreadFactory("TaskProcessing"));

    TaskTransferEngine(DistributedService svc) {
        this.svc = svc;
    }

    /**
     * Makes sure this peer has a running transfer thread, creating and starting one if it doesn't
     * yet -- called for every peer in PeerReadinessEngine.getReadyPeers(). Repeated calls for the
     * same peer keep its existing thread instead of starting another.
     */
    void ensurePeerWorker(String peerId) {
        startOrReusePeerWorker(peerId);
    }

    /** Returns this peer's running transfer thread, creating and starting one if it has none. */
    private PeerTransferWorker startOrReusePeerWorker(String peerId) {
        return mPeerWorkers.compute(peerId, (id, existing) -> {
            if (existing != null && existing.isRunning()) {
                EventLog.d(TAG, "Transfer thread PeerTransfer-" + id + " already running; reusing it");
                return existing;
            }
            EventLog.i(TAG, (existing != null ? "Transfer thread PeerTransfer-" + id + " was stopped"
                    : "No transfer thread for peer " + id + " yet") + "; starting one");
            return new PeerTransferWorker(id);
        });
    }

    /**
     * Stops every peer's transfer thread -- dropping the tasks still queued on each and
     * interrupting any upload in progress -- and forgets them, so the next getReadyPeers() starts
     * fresh ones. Called from PeerReadinessEngine.stopPeerDiscovery(), which waits up to
     * waitMillis for the threads to finish so that no upload is still running when its release
     * notices go out, and from DistributedService.onDestroy() with 0 (no waiting on the main
     * thread).
     */
    void shutdownPeerWorkers(long waitMillis) {
        List<PeerTransferWorker> stopped = new ArrayList<>();
        for (Map.Entry<String, PeerTransferWorker> entry : mPeerWorkers.entrySet()) {
            if (mPeerWorkers.remove(entry.getKey(), entry.getValue())) {
                entry.getValue().shutdown();
                stopped.add(entry.getValue());
            }
        }
        EventLog.i(TAG, "Stopped " + stopped.size() + " peer transfer thread(s)" + (waitMillis > 0
                ? "; waiting up to " + waitMillis + " ms for them to finish" : "; not waiting for them"));
        if (waitMillis <= 0) {
            return;
        }
        // All threads were told to stop above, so they wind down in parallel; wait for them here.
        long stopStartMillis = SystemClock.uptimeMillis();
        long giveUpAtMillis = stopStartMillis + waitMillis;
        for (PeerTransferWorker worker : stopped) {
            if (worker.awaitStopped(Math.max(0L, giveUpAtMillis - SystemClock.uptimeMillis()))) {
                EventLog.i(TAG, "Transfer thread PeerTransfer-" + worker.getPeerId() + " finished "
                        + (SystemClock.uptimeMillis() - stopStartMillis) + " ms after being stopped");
            } else {
                EventLog.w(TAG, "Transfer thread PeerTransfer-" + worker.getPeerId() + " didn't finish within "
                        + waitMillis + " ms of being stopped");
            }
        }
    }

    /**
     * Starts the receiver's two threads -- the task-processing thread and ResultTransferEngine's
     * result-transfer thread -- so both are already running when a sender's tasks start to arrive.
     * Called from PeerReadinessEngine.sendReadyAck(), just before this device tells a sender it's
     * ready. Each start first checks whether its thread is already running and only starts it if
     * not: the processing thread stays up across sessions, while the result-transfer thread is
     * stopped on every release notice (PeerReadinessEngine.handleReleaseReceived()) and started
     * again here for the next sender. Even without this, a task arriving first would still be
     * processed: execute() starts the executor's one thread on first use.
     */
    void startReceiverThreads() {
        if (mProcessingExecutor.getPoolSize() == 0) {
            mProcessingExecutor.prestartCoreThread();
        } else {
            EventLog.d(TAG, "Task processing thread already running; reusing it");
        }
        svc.mResultEngine.startResultTransferThread();
    }

    /** Stops the task-processing thread -- called from DistributedService.onDestroy(). */
    void shutdownProcessingThread() {
        boolean processing = mProcessingExecutor.getActiveCount() > 0;
        int dropped = mProcessingExecutor.shutdownNow().size();
        EventLog.i(TAG, "Stopping task processing thread: " + dropped + " queued task(s) dropped"
                + (processing ? ", one still being processed" : ""));
    }

    /**
     * Entry point for NetworkInterfaceServiceImpl.submitDistinctTasks(). Reuses the same gRPC
     * transfer engine performAttachmentTransfer used (per-peer channel, FileChunk streaming,
     * backpressure, keepalive) -- each DistinctTaskEnvelope is Parcel-marshalled to bytes "as
     * is" and streamed to the peer named in its own targetPeerId field, one gRPC stream per
     * envelope (submitDistinctTasks dispatches N distinct tasks to N distinct peers in
     * parallel, not one batch to one peer -- each on its target peer's own transfer thread, see
     * PeerTransferWorker, so tasks to different peers upload at the same time while tasks to the
     * same peer go one after another, with at most one waiting; a task for a peer that already
     * has one waiting is reported failed right away). All slices in one call share the same
     * rootGroupId (see DistinctTaskEnvelope.rootGroupId's own doc: "grouping all N slices of the
     * job"), so a single TaskGroup (see ResultTransferEngine) tracks them all -- as each slice's
     * result comes back through onResultReceived(), it's forwarded immediately via
     * callback.onPartialPeerResult() (see its own doc for why, not onAllTasksCompleted());
     * TaskGroup just tracks when every slice has reported in so it can stop tracking that
     * rootGroupId.
     */
    void submitDistinctTasks(List<DistinctTaskEnvelope> distinctTaskList, ITaskResponseCallback callback) {
        if (!distinctTaskList.isEmpty()) {
            String rootGroupId = distinctTaskList.get(0).rootGroupId;
            svc.mResultEngine.registerTaskGroup(rootGroupId, callback, distinctTaskList.size());
        }
        for (DistinctTaskEnvelope task : distinctTaskList) {
            if (task.targetPeerId == null) {
                EventLog.e(TAG, "Cannot submit task " + task.taskId + ": it has no targetPeerId.");
                svc.mResultEngine.reportPeerTaskFailed(task.rootGroupId, task.taskId, null,
                        ResultTransferEngine.ERROR_PEER_UNKNOWN, "Task has no target peer.");
                continue;
            }

            PeerTransferWorker worker = mPeerWorkers.get(task.targetPeerId);
            if (worker == null || !worker.isRunning()) {
                // getReadyPeers() normally started it already; if not, start it now so every
                // transfer to this peer still goes through the peer's one thread.
                EventLog.w(TAG, "Task " + task.taskId + ": no running transfer thread for "
                        + task.targetPeerId + " (not started by getReadyPeers(), or stopped since)");
                worker = startOrReusePeerWorker(task.targetPeerId);
            }

            // Marks the submit instant so ResultTransferEngine can compute this slice's round-trip
            // time once its result (or failure) comes back -- see recordTaskSent()/logRoundTrip().
            // Taken here rather than when the upload starts, so time spent waiting behind earlier
            // tasks for the same peer still counts, as it does for the caller.
            svc.mResultEngine.recordTaskSent(task.taskId);
            long queuedAtMillis = SystemClock.uptimeMillis();

            boolean accepted = worker.submit(() -> {
                EventLog.i(TAG, "Uploading task " + task.taskId + " to " + task.targetPeerId + " after "
                        + (SystemClock.uptimeMillis() - queuedAtMillis) + " ms queued");
                try {
                    sendDistinctTask(task);
                } catch (RuntimeException e) {
                    // sendDistinctTask() catches what it expects to fail; this only keeps an
                    // unforeseen RuntimeException from killing the peer's thread -- and, uncaught
                    // on any thread, the whole process.
                    EventLog.e(TAG, "Unexpected error sending task " + task.taskId + " to " + task.targetPeerId, e);
                    svc.mResultEngine.reportPeerTaskFailed(task.rootGroupId, task.taskId, task.targetPeerId,
                            ResultTransferEngine.ERROR_TRANSFER_EXCEPTION, String.valueOf(e.getMessage()));
                }
            });
            if (!accepted) {
                // Either the peer's queue is full -- one task uploading and another already waiting
                // (see PeerTransferWorker.MAX_QUEUED_TASKS) -- or its thread has been stopped.
                boolean queueFull = worker.isRunning();
                int errorCode = queueFull ? ResultTransferEngine.ERROR_PEER_BUSY
                        : ResultTransferEngine.ERROR_TRANSFER_FAILED;
                String reason = queueFull ? "Transfer queue for the peer is full"
                        : "Transfer thread for the peer is stopped";
                EventLog.w(TAG, "Cannot submit task " + task.taskId + " to " + task.targetPeerId + ": " + reason);
                // Nothing will upload it now: free its payload's shared memory right away rather
                // than whenever the garbage collector gets to it.
                if (task.payloadSharedMemory != null) {
                    task.payloadSharedMemory.close();
                }
                svc.mResultEngine.reportPeerTaskFailed(task.rootGroupId, task.taskId, task.targetPeerId,
                        errorCode, reason);
            }
        }
    }

    private void sendDistinctTask(DistinctTaskEnvelope task) {
        // Mirrors onDataReceived()'s log line exactly, field for field, so the sender's outgoing
        // log and the receiver's incoming log can be diffed directly to confirm the same data
        // actually made it across.
        EventLog.i(TAG, "sendDistinctTask: rootGroupId=" + task.rootGroupId
                + ", taskId=" + task.taskId
                + ", originatorNodeId=" + task.originatorNodeId
                + ", targetPeerId=" + task.targetPeerId
                + ", capability=" + task.capability
                + ", sliceIndex=" + task.sliceIndex
                + ", totalSlices=" + task.totalSlices
                + ", hopCount=" + task.hopCount
                + ", maxHops=" + task.maxHops
                + ", metadataParams=" + (task.metadataParams != null
                ? task.metadataParams.length + " byte(s)" : "null")
                + ", payloadSharedMemory=" + (task.payloadSharedMemory != null ? "present" : "null"));

        assert task.targetPeerId != null;
        PeerInfo targetPeer = svc.mKnownDevices.get(task.targetPeerId);
        if (targetPeer == null) {
            EventLog.e(TAG, "Cannot submit task " + task.taskId + ": target peer " + task.targetPeerId + " is unknown.");
            svc.mResultEngine.reportPeerTaskFailed(task.rootGroupId, task.taskId, task.targetPeerId,
                    ResultTransferEngine.ERROR_PEER_UNKNOWN, "Target peer " + task.targetPeerId + " is unknown.");
            return;
        }

        try {
            // marshallEnvelope() must run inside this try: it used to run before this block and
            // its uncaught RuntimeException (Parcel.marshall() rejecting a SharedMemory/fd)
            // crashed the whole process, since an uncaught exception on any thread -- not just
            // main -- kills the app on Android.
            byte[] payload = marshallEnvelope(task);
            long transferStartMillis = SystemClock.uptimeMillis();

            ManagedChannel channel = svc.mGrpcEngine.getOrCreateChannel(targetPeer.hostAddress);
            FileTransferServiceGrpc.FileTransferServiceStub asyncStub =
                    FileTransferServiceGrpc.newStub(channel);

            CountDownLatch latch = new CountDownLatch(1);
            final boolean[] transferSuccess = new boolean[]{false};
            // Set just before this method cancels the call itself (see below), so onError() can tell
            // the CANCELLED that cancel produces apart from a real failure.
            AtomicBoolean cancelledHere = new AtomicBoolean(false);

            StreamObserver<TransferResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(TransferResponse value) {
                    transferSuccess[0] = value.getSuccess();
                }

                @Override
                public void onError(Throwable t) {
                    if (cancelledHere.get()) {
                        // Only our own cancel() below coming back -- its outcome is logged there.
                        EventLog.d(TAG, "gRPC task submission stream to " + task.targetPeerId
                                + " closed after this device cancelled it");
                    } else {
                        EventLog.e(TAG, "gRPC task submission stream failed to " + task.targetPeerId, t);
                    }
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
                    .setFileName(task.taskId)
                    .setFileSize(payload.length)
                    .setFileIndex(task.sliceIndex)
                    .setTotalFiles(task.totalSlices)
                    .build();

            // Bounded waits (see GrpcTransportEngine.awaitReady(observer, callDone, maxWaitMillis)):
            // this runs on the peer's own transfer thread, so an upload that could never finish
            // would otherwise hold up every task queued behind it for that peer.
            long maxStallMillis = TimeUnit.SECONDS.toMillis(GrpcTransportEngine.GRPC_TIMEOUT_SECONDS);
            boolean streamed = svc.mGrpcEngine.awaitReady(readyAwareObserver, latch, maxStallMillis);
            if (streamed) {
                requestObserver.onNext(FileChunk.newBuilder().setMetadata(meta).build());
            }

            for (int offset = 0; streamed && offset < payload.length; ) {
                int len = Math.min(GrpcTransportEngine.CHUNK_SIZE, payload.length - offset);
                streamed = svc.mGrpcEngine.awaitReady(readyAwareObserver, latch, maxStallMillis);
                if (streamed) {
                    requestObserver.onNext(FileChunk.newBuilder()
                            .setChunk(ByteString.copyFrom(payload, offset, len))
                            .build());
                    offset += len;
                }
            }

            boolean completedInTime = false;
            if (streamed) {
                requestObserver.onCompleted();
                try {
                    completedInTime = latch.await(GrpcTransportEngine.GRPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    // Kept for the check below instead of being logged as an exception.
                    Thread.currentThread().interrupt();
                }
            }
            // Interrupted = this peer's transfer thread is being stopped (PeerTransferWorker.shutdown()),
            // whether it was waiting to send or waiting for the peer's reply -- a normal part of
            // cleanup, not a failure worth a stack trace.
            boolean interrupted = Thread.currentThread().isInterrupted();
            if (!completedInTime) {
                // Given up on: cancel the call so it can't keep streaming over this peer's
                // connection alongside the next task in the peer's queue.
                cancelledHere.set(true);
                readyAwareObserver.cancel("Task upload to " + task.targetPeerId + " did not complete", null);
            }
            boolean success = completedInTime && transferSuccess[0];
            long transferMillis = SystemClock.uptimeMillis() - transferStartMillis;

            if (success) {
                EventLog.i(TAG, "Task " + task.taskId + " (" + payload.length + " bytes) delivered to " + task.targetPeerId);
                svc.mResultEngine.recordTransferTime(task.taskId, false, payload.length, transferMillis);
            } else if (interrupted) {
                EventLog.w(TAG, "Task " + task.taskId + " upload to " + task.targetPeerId + " stopped after "
                        + transferMillis + " ms: its transfer thread is shutting down");
                svc.mResultEngine.reportPeerTaskFailed(task.rootGroupId, task.taskId, task.targetPeerId,
                        ResultTransferEngine.ERROR_TRANSFER_FAILED,
                        "Upload stopped: transfer thread shutting down");
            } else {
                EventLog.e(TAG, "Task " + task.taskId + " delivery to " + task.targetPeerId + " failed or timed out");
                svc.mResultEngine.reportPeerTaskFailed(task.rootGroupId, task.taskId, task.targetPeerId,
                        ResultTransferEngine.ERROR_TRANSFER_FAILED, "Task delivery failed or timed out");
            }

        } catch (Exception e) {
            EventLog.e(TAG, "Exception submitting task " + task.taskId + " to " + task.targetPeerId, e);
            svc.mResultEngine.reportPeerTaskFailed(task.rootGroupId, task.taskId, task.targetPeerId,
                    ResultTransferEngine.ERROR_TRANSFER_EXCEPTION, String.valueOf(e.getMessage()));
        }
    }

    /**
     * Called once a DistinctTaskEnvelope has been fully received from a peer. Stub for now --
     * real task processing (invoking the local AISDK, reporting a result back via
     * sendResponseBackToRoot / ITaskResponseCallback) is a later step. This only confirms the
     * transfer itself works.
     */
    void onDataReceived(DistinctTaskEnvelope envelope) {
        EventLog.i(TAG, "onDataReceived: rootGroupId=" + envelope.rootGroupId //ocr_recognise
                + ", taskId=" + envelope.taskId
                + ", originatorNodeId=" + envelope.originatorNodeId
                + ", targetPeerId=" + envelope.targetPeerId
                + ", capability=" + envelope.capability
                + ", sliceIndex=" + envelope.sliceIndex
                + ", totalSlices=" + envelope.totalSlices
                + ", hopCount=" + envelope.hopCount
                + ", maxHops=" + envelope.maxHops
                + ", metadataParams=" + (envelope.metadataParams != null
                ? envelope.metadataParams.length + " byte(s)" : "null")
                + ", payloadSharedMemory=" + (envelope.payloadSharedMemory != null ? "present" : "null"));

        // asResult() (see processDistinctTask()) keeps originatorNodeId/targetPeerId unchanged
        // from the original request, so a result envelope looks identical to a task envelope on
        // the wire -- originatorNodeId is now populated correctly (the SDK asks this device for
        // its own node ID via getLocalNodeId() and stamps it in), but comparing it to this
        // device's own serial would still mean re-deriving something we already know for free.
        // ResultTransferEngine's TaskGroup map is simpler and just as reliable: it only ever
        // holds entries this exact device created via submitDistinctTasks(), so a rootGroupId
        // found there can only be a result coming back for a task *we* submitted.
        if (svc.mResultEngine.isTrackingGroup(envelope.rootGroupId)) {
            svc.mResultEngine.onResultReceived(envelope);
        } else {
            // Hands the task to the processing thread, which startReceiverThreads() already started
            // before this device's ready ack: execute() only adds the task to that one thread's
            // queue -- it never starts another thread (the executor is fixed at exactly one).
            try {
                mProcessingExecutor.execute(() -> processDistinctTask(envelope));
            } catch (RejectedExecutionException e) {
                // Only after shutdownProcessingThread() (onDestroy) -- nothing will process it now.
                EventLog.w(TAG, "Dropping taskId=" + envelope.taskId + ": the processing thread is stopped");
            }
        }
    }

    /**
     * Looks up the already-warmed LenderHandler for this task's capability (see
     * PeerReadinessEngine.prepareLenderHandler() -- warming now happens once, up front, triggered
     * by a ready check, not per task) and calls handle() on it, on the receiver's one processing
     * thread (see mProcessingExecutor). The result is then handed to ResultTransferEngine's
     * result-transfer thread (see queueResult()) instead of being sent inline, so this thread
     * moves straight on to the next task rather than waiting for the originator's reply.

     * The actual image/frame data lives in payloadSharedMemory (per its own field doc: "large AI
     * weights, tensors, images"), not metadataParams, so it's unwrapped to a byte[] via
     * SharedMemoryUtil before being handed to handle(). Everything here runs inside one
     * try/catch, since this method is invoked from a bare executor thread with nothing else
     * catching exceptions above it -- see the marshallEnvelope() crash this same gap just
     * caused on the sender side (an uncaught exception on any thread kills the whole process
     * on Android, not just that thread).
     */
    private void processDistinctTask(DistinctTaskEnvelope envelope) {
        try {
            LenderHandler handler = svc.mReadinessEngine.mLenderHandlers.get(envelope.capability);
            if (handler == null) {
                EventLog.e(TAG, "No LenderHandler available for capability '" + envelope.capability
                        + "', taskId=" + envelope.taskId);
                return;
            }

            byte[] request;
            if (envelope.payloadSharedMemory != null) {
                request = SharedMemoryUtil.unwrap(envelope.payloadSharedMemory);
            } else {
                EventLog.w(TAG, "No payloadSharedMemory for taskId=" + envelope.taskId
                        + "; calling handle() with a null request");
                request = null;
            }

            long processStartMillis = SystemClock.uptimeMillis();
            byte[] response;
            // Same lock as prepare()/dispose() (see PeerReadinessEngine), so a ready check or a
            // release can't run on this handler mid-handle(). A release may also have removed and
            // disposed it since the lookup above -- product.frame's handle() would then silently
            // re-load its model -- so the task is dropped instead of run on a disposed handler.
            synchronized (handler) {
                if (svc.mReadinessEngine.mLenderHandlers.get(envelope.capability) != handler) {
                    EventLog.w(TAG, "LenderHandler for capability '" + envelope.capability
                            + "' was released; dropping taskId=" + envelope.taskId);
                    return;
                }
                svc.mInferenceMeter.begin();
                try {
                    response = handler.handle(request);
                } finally {
                    svc.mInferenceMeter.end();
                }
            }
            long processingMillis = SystemClock.uptimeMillis() - processStartMillis;
            svc.mInferenceMeter.taskDone(processingMillis);
            long processingCount = mProcessingCount.incrementAndGet();
            long totalProcessingMillis = mTotalProcessingMillis.addAndGet(processingMillis);
            long avgProcessingMillis = totalProcessingMillis / processingCount;

            EventLog.i(TAG, "Processing is done. taskId=" + envelope.taskId
                    + ", response=" + (response != null ? response.length + " byte(s)" : "null")
                    + ", processing time=" + processingMillis + "ms (Avg processing time="
                    + avgProcessingMillis + "ms)");

            if (response != null) {
                // Built here, on the processing thread, from this task's own envelope: asResult()
                // copies its rootGroupId/taskId/sliceIndex, and SharedMemoryUtil.wrap() copies the
                // response into the result's own SharedMemory -- so the queued result can't pick up
                // another task's identity or bytes while it waits for the result-transfer thread.
                DistinctTaskEnvelope resultEnvelope = envelope.asResult(null, SharedMemoryUtil.wrap(response));
                svc.mResultEngine.queueResult(resultEnvelope);
            } else {
                EventLog.w(TAG, "No response to send back for taskId=" + envelope.taskId);
            }
        } catch (Exception e) {
            EventLog.e(TAG, "LenderHandler processing failed for taskId=" + envelope.taskId, e);
        }
    }

    /**
     * Converts a DistinctTaskEnvelope to bytes for network transport. payloadSharedMemory (a
     * file-descriptor reference) cannot go through Parcel.marshall() at all -- Android throws
     * "Can't marshal a Parcel that has Binder objects" if the parcel contains one, which is a
     * RuntimeException, and since this used to run outside any try/catch it crashed the whole
     * process (an uncaught exception on any thread, not just main, kills the app on Android).
     * So it's unwrapped to a plain byte[] via SharedMemoryUtil first, marshalled separately from
     * a wire-safe copy of the envelope with payloadSharedMemory stripped, and the two byte
     * arrays are length-prefixed together. Declared "throws Exception" so the caller
     * (sendDistinctTask) is forced to keep this inside its own try/catch.
     */
    static byte[] marshallEnvelope(DistinctTaskEnvelope envelope) throws Exception {
        byte[] sharedMemBytes = null;
        if (envelope.payloadSharedMemory != null) {
            try {
                sharedMemBytes = SharedMemoryUtil.unwrap(envelope.payloadSharedMemory);
            } finally {
                envelope.payloadSharedMemory.close();
            }
        }

        DistinctTaskEnvelope wireSafe = new DistinctTaskEnvelope(
                envelope.rootGroupId, envelope.taskId, envelope.originatorNodeId, envelope.targetPeerId,
                envelope.capability, envelope.sliceIndex, envelope.totalSlices, envelope.hopCount,
                envelope.maxHops, envelope.metadataParams, null);

        Parcel parcel = Parcel.obtain();
        byte[] envelopeBytes;
        try {
            wireSafe.writeToParcel(parcel, 0);
            envelopeBytes = parcel.marshall();
        } finally {
            parcel.recycle();
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream data = new DataOutputStream(out);
        data.writeInt(envelopeBytes.length);
        data.write(envelopeBytes);
        if (sharedMemBytes != null) {
            data.writeInt(sharedMemBytes.length);
            data.write(sharedMemBytes);
        } else {
            data.writeInt(-1);
        }
        return out.toByteArray();
    }

    /**
     * Reconstructs a DistinctTaskEnvelope from marshalled bytes (the length-prefixed
     * envelope+shared-memory format marshallEnvelope() writes); null if decoding fails. Not
     * wrapped by a try/catch at its call site (onCompleted()), so every failure path here is
     * caught internally -- letting anything propagate would crash the process the same way the
     * sender-side marshall() bug just did.
     */
    static DistinctTaskEnvelope unmarshallEnvelope(byte[] data) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            int envelopeLength = in.readInt();
            byte[] envelopeBytes = new byte[envelopeLength];
            in.readFully(envelopeBytes);

            int sharedMemLength = in.readInt();
            SharedMemory sharedMemory = null;
            if (sharedMemLength >= 0) {
                byte[] sharedMemBytes = new byte[sharedMemLength];
                in.readFully(sharedMemBytes);
                sharedMemory = SharedMemoryUtil.wrap(sharedMemBytes);
            }

            Parcel parcel = Parcel.obtain();
            DistinctTaskEnvelope wireSafe;
            try {
                parcel.unmarshall(envelopeBytes, 0, envelopeBytes.length);
                parcel.setDataPosition(0);
                wireSafe = DistinctTaskEnvelope.CREATOR.createFromParcel(parcel);
            } finally {
                parcel.recycle();
            }

            if (sharedMemory == null) {
                return wireSafe;
            }
            return new DistinctTaskEnvelope(
                    wireSafe.rootGroupId, wireSafe.taskId, wireSafe.originatorNodeId, wireSafe.targetPeerId,
                    wireSafe.capability, wireSafe.sliceIndex, wireSafe.totalSlices, wireSafe.hopCount,
                    wireSafe.maxHops, wireSafe.metadataParams, sharedMemory);
        } catch (Exception e) {
            EventLog.e(TAG, "Failed to unmarshall DistinctTaskEnvelope", e);
            return null;
        }
    }
}
