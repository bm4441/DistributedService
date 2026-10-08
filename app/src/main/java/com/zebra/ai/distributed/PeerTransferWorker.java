package com.zebra.ai.distributed;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The sender-side transfer thread for one peer. Created when that peer is asked to get ready
 * (PeerReadinessEngine.getReadyPeers() -> TaskTransferEngine.ensurePeerWorker()) and kept for
 * every later distinct-task transfer to it until the sender stops discovery
 * (PeerReadinessEngine.stopPeerDiscovery() -> TaskTransferEngine.shutdownPeerWorkers()):
 * TaskTransferEngine.submitDistinctTasks() queues each task on its target peer's thread (see
 * submit()), so transfers to one peer run one after another on that peer's own thread while
 * different peers transfer in parallel. At most MAX_QUEUED_TASKS tasks wait behind the upload in
 * progress; a task arriving when the queue is already full is rejected instead of queued.
 */
final class PeerTransferWorker {

    private static final String TAG = "DistributedService";

    // How many tasks may wait behind the upload in progress. A task further back would mostly
    // start after the SDK has already given up on it -- it times a slice out after 2 s
    // (ocr.recognize) or 5 s (product.frame), per distributed-schema.json in the AI Suite AAR -- so
    // it's reported failed straight away instead (the SDK treats that the same as a timeout).
    static final int MAX_QUEUED_TASKS = 1;

    private final String mPeerId;
    // A single-thread executor rather than a bare Thread: it already provides the one thread, the
    // queue of work waiting for it, and a clean way to stop both.
    private final ThreadPoolExecutor mExecutor;

    PeerTransferWorker(String peerId) {
        mPeerId = peerId;
        mExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(MAX_QUEUED_TASKS),
                new LoggingThreadFactory("PeerTransfer-" + peerId));
        // Start the thread now, when the peer is asked to get ready, rather than on its first task.
        mExecutor.prestartCoreThread();
        EventLog.i(TAG, "Started transfer thread PeerTransfer-" + peerId + " (queue size "
                + MAX_QUEUED_TASKS + ")");
    }

    /** False once shutdown() has been called. */
    boolean isRunning() {
        return !mExecutor.isShutdown();
    }

    /**
     * Queues a transfer to run on this peer's thread, after the upload in progress. Returns false
     * if it can't be taken: MAX_QUEUED_TASKS transfers are already waiting, or the thread has been
     * stopped (isRunning() tells which).
     */
    boolean submit(Runnable transfer) {
        try {
            mExecutor.execute(transfer);
            return true;
        } catch (RejectedExecutionException e) {
            return false;
        }
    }

    String getPeerId() {
        return mPeerId;
    }

    /**
     * Stops this peer's transfer thread: tasks still waiting for it are dropped, and an upload in
     * progress is interrupted, which makes it stop at its next wait and cancel its call.
     */
    void shutdown() {
        boolean uploading = mExecutor.getActiveCount() > 0;
        int dropped = mExecutor.shutdownNow().size();
        EventLog.i(TAG, "Stopping transfer thread PeerTransfer-" + mPeerId + ": " + dropped
                + " queued task(s) dropped, " + (uploading ? "interrupting the upload in progress"
                : "no upload in progress"));
    }

    /** After shutdown(): waits up to timeoutMillis for the thread to finish; true if it has. */
    boolean awaitStopped(long timeoutMillis) {
        try {
            return mExecutor.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
