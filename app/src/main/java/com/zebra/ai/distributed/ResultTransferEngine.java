package com.zebra.ai.distributed;

import android.os.RemoteException;
import android.os.SystemClock;

import com.google.protobuf.ByteString;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.grpc.ManagedChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Result transfer: sending a processed task's result envelope back to whoever submitted it, and
 * receiving/dispatching results that come back for this device's own submitted tasks. Split out
 * of DistributedService verbatim -- see DistributedService's own fields/methods for the shared
 * state this reaches into (mService).
 */
final class ResultTransferEngine {

    private static final String TAG = "DistributedService";

    /* ================= ITaskResponseCallback.onPeerTaskFailed error codes ================= */
    static final int ERROR_PEER_UNKNOWN = 1;
    static final int ERROR_TRANSFER_FAILED = 2;
    static final int ERROR_TRANSFER_EXCEPTION = 3;
    // The peer's transfer queue was full, so the task was never sent -- see
    // PeerTransferWorker.MAX_QUEUED_TASKS.
    static final int ERROR_PEER_BUSY = 4;
    static final int ERROR_SERVICE_PAUSED = 5;

    // One entry per in-flight submitDistinctTasks() call, keyed by the shared rootGroupId all of
    // its slices carry -- see TaskGroup/onResultReceived().
    private final ConcurrentHashMap<String, TaskGroup> mTaskGroups = new ConcurrentHashMap<>();

    // Send timestamp per taskId (not rootGroupId -- different slices of the same group can go to
    // different peers with different latencies), recorded in recordTaskSent() and consumed once
    // in onResultReceived() to compute that slice's round-trip time. Also removed on a reported
    // failure (see reportPeerTaskFailed()), since a slice that will never return shouldn't leak
    // its entry here forever.
    private final ConcurrentHashMap<String, Long> mTaskSentAtMillis = new ConcurrentHashMap<>();
    // Running sum/count backing the all-time average round-trip time logged on every result --
    // see onResultReceived(). Only successful round trips count; failures don't.
    private final AtomicLong mResultCount = new AtomicLong(0);
    private final AtomicLong mTotalRoundTripMillis = new AtomicLong(0);

    // Running sum/count backing the all-time average gRPC data-transfer time logged on every
    // successful transfer -- see recordTransferTime(). Shared between outgoing task sends
    // (TaskTransferEngine.sendDistinctTask()) and outgoing result sends (sendResultEnvelope()
    // below), since both are the same kind of operation: a marshalled envelope streamed to a
    // peer over gRPC.
    private final AtomicLong mTransferCount = new AtomicLong(0);
    private final AtomicLong mTotalTransferMillis = new AtomicLong(0);
    // Bytes and time behind the average throughput in that same log line -- kept apart for task
    // uploads and result uploads, unlike the average time: a multi-MB frame and a few-KB result
    // (mostly round-trip latency) move at such different effective rates that one combined figure
    // would describe neither.
    private final AtomicLong mTaskTransferBytes = new AtomicLong(0);
    private final AtomicLong mTaskTransferMillis = new AtomicLong(0);
    private final AtomicLong mResultTransferBytes = new AtomicLong(0);
    private final AtomicLong mResultTransferMillis = new AtomicLong(0);

    // The receiver's one result-transfer thread -- see queueResult(). Results used to be sent
    // inline on the processing thread, which then waited for the originator's reply before it
    // could start on the next task; now the processing thread only queues each result here and
    // moves on. A fresh one per session: started just before each ready ack
    // (TaskTransferEngine.startReceiverThreads()) and stopped when the sender releases this device
    // (PeerReadinessEngine.handleReleaseReceived()) -- an executor can't be restarted once it's
    // shut down, hence a new one each time. null while stopped; guarded by `this`.
    private ThreadPoolExecutor mResultTransferExecutor;

    private final DistributedService svc;

    ResultTransferEngine(DistributedService svc) {
        this.svc = svc;
    }

    /**
     * Starts the result-transfer thread if it isn't already running -- called from
     * TaskTransferEngine.startReceiverThreads(), just before each ready ack. A call while it's
     * already running does nothing, so there's never more than one.
     */
    synchronized void startResultTransferThread() {
        if (mResultTransferExecutor != null) {
            EventLog.d(TAG, "Result transfer thread already running; reusing it");
            return;
        }
        mResultTransferExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new LoggingThreadFactory("ResultTransfer"));
        mResultTransferExecutor.prestartCoreThread();
        EventLog.i(TAG, "Started result transfer thread");
    }

    /**
     * Queues a finished task's result for the result-transfer thread -- called from
     * TaskTransferEngine.processDistinctTask() in place of sending it inline. The envelope was
     * built by asResult() from that task's own envelope (same rootGroupId/taskId/sliceIndex) with
     * the response copied into its own SharedMemory, so it can't be mixed up with another task's
     * result while it waits. Results are sent one at a time, in the order they were produced.
     */
    void queueResult(DistinctTaskEnvelope resultEnvelope) {
        ThreadPoolExecutor executor;
        synchronized (this) {
            executor = mResultTransferExecutor;
        }
        if (executor == null) {
            // Stopped by a release notice (or onDestroy): the sender no longer wants results.
            EventLog.w(TAG, "Dropping result for taskId=" + resultEnvelope.taskId
                    + ": the result transfer thread is stopped");
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    sendResultEnvelope(resultEnvelope);
                } catch (RuntimeException e) {
                    // sendResultEnvelope() catches what it expects to fail; this only keeps an
                    // unforeseen RuntimeException from killing the result thread -- and, uncaught
                    // on any thread, the whole process.
                    EventLog.e(TAG, "Unexpected error sending result for taskId=" + resultEnvelope.taskId, e);
                }
            });
        } catch (RejectedExecutionException e) {
            // Stopped between the check above and here -- nothing will send it now.
            EventLog.w(TAG, "Dropping result for taskId=" + resultEnvelope.taskId
                    + ": the result transfer thread is stopped");
        }
    }

    /**
     * Stops the result-transfer thread, dropping any results still waiting to be sent -- called
     * from PeerReadinessEngine.handleReleaseReceived() (the sender has released this device and no
     * longer wants them) and from DistributedService.onDestroy(). The next ready ack starts a
     * fresh one.
     */
    synchronized void shutdownResultTransferThread() {
        if (mResultTransferExecutor == null) {
            EventLog.d(TAG, "Result transfer thread already stopped");
            return;
        }
        boolean sending = mResultTransferExecutor.getActiveCount() > 0;
        int dropped = mResultTransferExecutor.shutdownNow().size();
        mResultTransferExecutor = null;
        EventLog.i(TAG, "Stopping result transfer thread: " + dropped + " queued result(s) dropped, "
                + (sending ? "interrupting the send in progress" : "no send in progress"));
    }

    /**
     * Stops the result-transfer thread after it has sent the results already queued: no new result is queued from now on,
     * those waiting are sent, and the thread is released. Waits up to waitMillis; a send still going on then is interrupted
     * and the results behind it are dropped. Called when the service becomes a Requestor, after its processing thread has
     * finished (PeerReadinessEngine.leaveContributorRole()), so that the results of the tasks it completed reach their
     * originator. Returns true if every queued result was sent. Blocks, so not on the main thread.
     */
    boolean finishResultTransferThread(long waitMillis) {
        ThreadPoolExecutor executor;
        synchronized (this) {
            executor = mResultTransferExecutor;
            mResultTransferExecutor = null;
        }
        if (executor == null) {
            EventLog.d(TAG, "Result transfer thread already stopped");
            return true;
        }
        int pending = executor.getQueue().size() + executor.getActiveCount();
        executor.shutdown();                          // takes no new result; the queued ones are still sent
        EventLog.i(TAG, "Result transfer thread: sending " + pending + " queued result(s) before it is released");
        try {
            if (executor.awaitTermination(waitMillis, TimeUnit.MILLISECONDS)) {
                EventLog.i(TAG, "Result transfer thread released: every queued result is sent");
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        int dropped = executor.shutdownNow().size();
        EventLog.w(TAG, "Result transfer thread released: the queued results were not all sent within " + waitMillis
                + " ms (" + dropped + " dropped, the send in progress interrupted)");
        return false;
    }

    /** Registers a new TaskGroup for a rootGroupId -- called from TaskTransferEngine.submitDistinctTasks(). */
    void registerTaskGroup(String rootGroupId, ITaskResponseCallback callback, int expectedCount) {
        mTaskGroups.put(rootGroupId, new TaskGroup(callback, expectedCount));
    }

    /** True if this device itself submitted the task group with this rootGroupId. */
    boolean isTrackingGroup(String rootGroupId) {
        return mTaskGroups.containsKey(rootGroupId);
    }

    /**
     * Marks the moment a task is submitted (TaskTransferEngine.submitDistinctTasks(), before it
     * waits on its peer's transfer thread), so onResultReceived() can later compute that slice's
     * full round-trip time (submit -> upload -> peer processing -> result back) once its result
     * arrives.
     */
    void recordTaskSent(String taskId) {
        mTaskSentAtMillis.put(taskId, SystemClock.uptimeMillis());
    }

    /**
     * Records one successful gRPC data transfer -- called from both
     * TaskTransferEngine.sendDistinctTask() (isResult=false) and sendResultEnvelope() below
     * (isResult=true) -- and logs its time, payload size and throughput (payload / transfer time,
     * so it includes waiting for the connection and the peer's reply, not just the streaming),
     * alongside the running all-time average time across both kinds of transfer combined and the
     * average throughput (total bytes / total time) of this kind.
     */
    void recordTransferTime(String taskId, boolean isResult, long payloadBytes, long transferMillis) {
        long count = mTransferCount.incrementAndGet();
        long totalMillis = mTotalTransferMillis.addAndGet(transferMillis);
        long averageMillis = totalMillis / count;
        long kindBytes = (isResult ? mResultTransferBytes : mTaskTransferBytes).addAndGet(payloadBytes);
        long kindMillis = (isResult ? mResultTransferMillis : mTaskTransferMillis).addAndGet(transferMillis);
        EventLog.i(TAG, "taskId=" + taskId + (isResult ? " result transfer time=" : " transfer time=")
                + transferMillis + "ms, payload=" + GrpcTransportEngine.formatSize(payloadBytes)
                + ", throughput=" + GrpcTransportEngine.formatThroughput(payloadBytes, transferMillis)
                + " (Avg transfer time=" + averageMillis + "ms, Avg " + (isResult ? "result" : "task")
                + " throughput=" + GrpcTransportEngine.formatThroughput(kindBytes, kindMillis) + ")");
    }

    /**
     * Sends a result envelope (built via DistinctTaskEnvelope.asResult(), which keeps
     * originatorNodeId/targetPeerId unchanged from the original task) back to whoever sent this
     * device the original task -- resultEnvelope.originatorNodeId, now that the SDK's own
     * DistributedClient actually fills it in correctly (it asks this device for its own node ID
     * via the newer INetworkInterfaceService.getLocalNodeId() and stamps that into
     * originatorNodeId when it builds the task envelope in the first place). Previously that
     * field was left blank by older SDK versions, so this used to route by the wire-level
     * FileMetadata.sender_serial the task itself arrived with instead -- no longer needed.
     * Otherwise this mirrors sendDistinctTask() exactly: same marshalling, same gRPC
     * channel/streaming mechanics, same FileMetadata.file_name convention (the taskId), so the
     * receiving end's onDataReceived() decodes it exactly like any other envelope -- telling a
     * result apart from a fresh task is onDataReceived()'s job (via mTaskGroups), not the wire
     * format's.
     */
    void sendResultEnvelope(DistinctTaskEnvelope resultEnvelope) {
        String targetSerial = resultEnvelope.originatorNodeId;
        PeerInfo originator = svc.mKnownDevices.get(targetSerial);
        if (originator == null) {
            EventLog.e(TAG, "Cannot send result for taskId=" + resultEnvelope.taskId + ": originator "
                    + targetSerial + " is unknown.");
            return;
        }

        try {
            byte[] payload = TaskTransferEngine.marshallEnvelope(resultEnvelope);
            long transferStartMillis = SystemClock.uptimeMillis();

            ManagedChannel channel = svc.mGrpcEngine.getOrCreateChannel(originator.hostAddress);
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
                        EventLog.d(TAG, "gRPC result submission stream to " + targetSerial
                                + " closed after this device cancelled it");
                    } else {
                        GrpcTransportEngine.logCallFailure("gRPC result submission stream failed to " + targetSerial, t);
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
                    .setFileName(resultEnvelope.taskId)
                    .setFileSize(payload.length)
                    .setFileIndex(resultEnvelope.sliceIndex)
                    .setTotalFiles(resultEnvelope.totalSlices)
                    .build();

            // Bounded waits (see GrpcTransportEngine.awaitReady(observer, callDone, maxWaitMillis)):
            // this runs on the one result-transfer thread, so a send that could never finish would
            // otherwise hold up every result queued behind it.
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
            // Interrupted = the result-transfer thread is being stopped (shutdownResultTransferThread(),
            // on a release notice or onDestroy), whether it was waiting to send or waiting for the
            // originator's reply -- a normal part of cleanup, not a failure worth a stack trace.
            boolean interrupted = Thread.currentThread().isInterrupted();
            if (!completedInTime) {
                // Given up on: cancel the call rather than leave it open behind the next result.
                cancelledHere.set(true);
                readyAwareObserver.cancel("Result upload to " + targetSerial + " did not complete", null);
            }
            boolean success = completedInTime && transferSuccess[0];
            long transferMillis = SystemClock.uptimeMillis() - transferStartMillis;

            if (success) {
                EventLog.i(TAG, "Result for taskId=" + resultEnvelope.taskId + " (" + payload.length
                        + " bytes) delivered to " + targetSerial);
                recordTransferTime(resultEnvelope.taskId, true, payload.length, transferMillis);
            } else if (interrupted) {
                EventLog.w(TAG, "Result for taskId=" + resultEnvelope.taskId + " upload to " + targetSerial
                        + " stopped after " + transferMillis
                        + " ms: the result transfer thread is shutting down");
            } else {
                EventLog.e(TAG, "Result for taskId=" + resultEnvelope.taskId + " delivery to "
                        + targetSerial + " failed or timed out");
            }
        } catch (Exception e) {
            EventLog.e(TAG, "Exception sending result for taskId=" + resultEnvelope.taskId
                    + " to " + targetSerial, e);
        }
    }

    /**
     * Called when a result envelope (see TaskTransferEngine.processDistinctTask()'s
     * asResult()/sendResultEnvelope()) comes back from the peer that contributed the actual work
     * for one of this device's own submitted tasks. Forwards it immediately via
     * onPartialPeerResult() -- NOT batched into an onAllTasksCompleted() call once every slice is
     * in, despite that sounding like the right name for it. Decompiling the SDK's own
     * ITaskResponseCallback implementation (DistributedClient$Group$1) showed
     * onAllTasksCompleted()'s entire body just fails the whole task group with an IOException
     * ("closed before this slice returned") -- it's an abort signal, not a success path.
     * onPartialPeerResult() is what the SDK actually expects per slice, as each one arrives; its
     * own Group class accumulates them internally and resolves the caller's future once
     * satisfied. TaskGroup here now exists only to look up which callback a rootGroupId belongs
     * to, and to know when to stop tracking it.
     */
    void onResultReceived(DistinctTaskEnvelope envelope) {
        EventLog.i(TAG, "Result generated from contributor " + envelope.targetPeerId
                + " for taskId=" + envelope.taskId);
        logRoundTrip(envelope.taskId);

        TaskGroup group = mTaskGroups.get(envelope.rootGroupId);
        if (group == null) {
            EventLog.w(TAG, "No pending task group for rootGroupId=" + envelope.rootGroupId
                    + "; dropping result for taskId=" + envelope.taskId);
            return;
        }

        safeOnPartialPeerResult(group.callback, envelope.rootGroupId, envelope.targetPeerId, envelope);
        markSliceReported(envelope.rootGroupId, group);
    }

    /**
     * Computes this taskId's round-trip time (send -> result) and logs it alongside the running
     * all-time average across every successful result so far. Silently does nothing if no send
     * timestamp was recorded for this taskId (e.g. the service restarted mid-flight).
     */
    private void logRoundTrip(String taskId) {
        Long sentAtMillis = mTaskSentAtMillis.remove(taskId);
        if (sentAtMillis == null) {
            return;
        }
        long roundTripMillis = SystemClock.uptimeMillis() - sentAtMillis;
        long count = mResultCount.incrementAndGet();
        long totalMillis = mTotalRoundTripMillis.addAndGet(roundTripMillis);
        long averageMillis = totalMillis / count;
        EventLog.i(TAG, "Round-trip for taskId=" + taskId + ": " + roundTripMillis
                + " ms (running average over " + count + " result(s): " + averageMillis + " ms)");
    }

    /**
     * Entry point for NetworkInterfaceServiceImpl.submitTaskToPeer()/sendDistinctTask() delivery
     * failures -- a peer this device tried to hand a slice to that turned out to be unknown, or
     * whose gRPC transfer failed/timed out/threw. Reports it via onPeerTaskFailed() so the SDK's
     * own Group knows that slice will never arrive, instead of it waiting forever. Also drops
     * this taskId's pending send timestamp (see recordTaskSent()/logRoundTrip()) -- a slice that
     * failed will never get a result, so it must not linger in mTaskSentAtMillis forever, and it
     * shouldn't count toward the round-trip average either way.
     */
    void reportPeerTaskFailed(String rootGroupId, String taskId, String peerId, int errorCode, String errorMessage) {
        mTaskSentAtMillis.remove(taskId);

        TaskGroup group = mTaskGroups.get(rootGroupId);
        if (group == null) {
            EventLog.w(TAG, "No pending task group for rootGroupId=" + rootGroupId
                    + "; dropping failure report for peer " + peerId);
            return;
        }

        safeOnPeerTaskFailed(group.callback, rootGroupId, peerId, errorCode, errorMessage);
        markSliceReported(rootGroupId, group);
    }

    /**
     * Entry point for NetworkInterfaceServiceImpl.cancelTaskGroup(). Unlike onResultReceived()/
     * reportPeerTaskFailed() (per-slice), this ends the whole group at once via
     * onAllTasksCompleted() -- which per its own doc is the SDK's actual abort signal, not a
     * "here are your successful results" signal. Appropriate here specifically because canceling
     * *is* an abnormal, caller-requested early termination, unlike a slice merely succeeding or
     * failing on its own.
     */
    void cancelTaskGroup(String rootGroupId) {
        TaskGroup group = mTaskGroups.remove(rootGroupId);
        if (group == null) {
            EventLog.w(TAG, "cancelTaskGroup: no pending task group for rootGroupId=" + rootGroupId);
            return;
        }
        safeOnAllTasksCompleted(group.callback, rootGroupId);
    }

    /**
     * The service is paused by its policy: every task group still waiting for results is ended the way a cancellation ends
     * it (see cancelTaskGroup()), so the application is not left waiting for results that will not come.
     */
    void cancelAllTaskGroups() {
        for (String rootGroupId : new java.util.ArrayList<>(mTaskGroups.keySet())) {
            cancelTaskGroup(rootGroupId);
        }
    }

    /** Shared bookkeeping for both success (onResultReceived) and failure (reportPeerTaskFailed) paths. */
    private void markSliceReported(String rootGroupId, TaskGroup group) {
        int reported = group.reportedCount.incrementAndGet();
        if (reported >= group.expectedCount) {
            mTaskGroups.remove(rootGroupId);
        } else {
            EventLog.d(TAG, "rootGroupId=" + rootGroupId + ": " + reported + "/"
                    + group.expectedCount + " slice(s) reported so far");
        }
    }

    /**
     * Delivers one slice's result via onPartialPeerResult() -- see onResultReceived()'s doc for
     * why this is used instead of onAllTasksCompleted(), despite the latter's name sounding like
     * the right fit for "all slices are in."
     */
    private static void safeOnPartialPeerResult(ITaskResponseCallback callback, String rootGroupId,
                                                String peerId, DistinctTaskEnvelope resultEnvelope) {
        if (callback == null) {
            EventLog.w(TAG, "Result for rootGroupId=" + rootGroupId + " from " + peerId
                    + ", but no callback was registered to deliver it to");
            return;
        }
        try {
            callback.onPartialPeerResult(rootGroupId, peerId, resultEnvelope);
            EventLog.i(TAG, "onPartialPeerResult delivered for rootGroupId=" + rootGroupId + " from " + peerId);
        } catch (RemoteException e) {
            EventLog.w(TAG, "Remote callback failure in onPartialPeerResult for rootGroupId=" + rootGroupId, e);
        }
    }

    /** Delivers one slice's delivery failure via onPeerTaskFailed(). */
    private static void safeOnPeerTaskFailed(ITaskResponseCallback callback, String rootGroupId,
                                             String peerId, int errorCode, String errorMessage) {
        if (callback == null) {
            EventLog.w(TAG, "Peer " + peerId + " failed for rootGroupId=" + rootGroupId + " (" + errorMessage
                    + "), but no callback was registered to deliver it to");
            return;
        }
        try {
            callback.onPeerTaskFailed(rootGroupId, peerId, errorCode, errorMessage);
            EventLog.i(TAG, "onPeerTaskFailed delivered for rootGroupId=" + rootGroupId + ", peer=" + peerId);
        } catch (RemoteException e) {
            EventLog.w(TAG, "Remote callback failure in onPeerTaskFailed for rootGroupId=" + rootGroupId, e);
        }
    }

    /**
     * Delivers a cancellation via onAllTasksCompleted() -- the SDK's own implementation
     * (DistributedClient$Group$1, decompiled) ignores the results list entirely and just treats
     * any call to this method as "close with an IOException," so an empty list is passed
     * deliberately; its contents have no effect either way.
     */
    private static void safeOnAllTasksCompleted(ITaskResponseCallback callback, String rootGroupId) {
        if (callback == null) {
            EventLog.w(TAG, "Cancelled rootGroupId=" + rootGroupId
                    + ", but no callback was registered to deliver it to");
            return;
        }
        try {
            callback.onAllTasksCompleted(rootGroupId, Collections.emptyList());
            EventLog.i(TAG, "onAllTasksCompleted (cancellation) delivered for rootGroupId=" + rootGroupId);
        } catch (RemoteException e) {
            EventLog.w(TAG, "Remote callback failure in onAllTasksCompleted for rootGroupId=" + rootGroupId, e);
        }
    }

    /**
     * Tracks one in-flight submitDistinctTasks() call while its slices' results are still
     * trickling in from their respective peers (see onResultReceived()/reportPeerTaskFailed()) --
     * expectedCount is the number of slices dispatched, so the group is complete once that many
     * slices have each either succeeded or failed.
     */
    private static final class TaskGroup {
        final ITaskResponseCallback callback;
        final int expectedCount;
        final AtomicInteger reportedCount = new AtomicInteger(0);

        TaskGroup(ITaskResponseCallback callback, int expectedCount) {
            this.callback = callback;
            this.expectedCount = expectedCount;
        }
    }
}
