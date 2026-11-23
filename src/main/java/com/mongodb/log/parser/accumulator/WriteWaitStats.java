package com.mongodb.log.parser.accumulator;

import com.mongodb.log.parser.SlowQuery;

/**
 * Write-side cost of slow operations, from fields mongod logs on most slow writes (8.0):
 *
 * - storage.data.txnBytesDirty: bytes the operation dirtied in the WiredTiger cache. This, not storage.data.bytesWritten,
 *   measures how much a write wrote: bytesWritten is logged only when the operation itself had to write pages to disk
 *   (reconciliation or eviction in the request thread), which is rare.
 * - flowControl.timeAcquiringMicros: time waiting for flow-control tickets, i.e. throttled because majority-committed
 *   replication fell behind.
 * - waitForWriteConcernDurationMillis: time waiting for the write concern (e.g. w:majority) after the write itself.
 *
 * Averages are over the operations that logged the field (counts are kept), never over all operations.
 */
public class WriteWaitStats {

    private long dirtyOps;
    private long totalDirtyBytes;
    private long maxDirtyBytes;
    private long flowControlOps;
    private long totalFlowControlMicros;
    private long writeConcernOps;
    private long totalWriteConcernMillis;

    public void add(SlowQuery q) {
        if (q.txnBytesDirty != null) {
            dirtyOps++;
            totalDirtyBytes += q.txnBytesDirty;
            if (q.txnBytesDirty > maxDirtyBytes) {
                maxDirtyBytes = q.txnBytesDirty;
            }
        }
        if (q.flowControlWaitMicros != null) {
            flowControlOps++;
            totalFlowControlMicros += q.flowControlWaitMicros;
        }
        if (q.writeConcernWaitMillis != null) {
            writeConcernOps++;
            totalWriteConcernMillis += q.writeConcernWaitMillis;
        }
    }

    public long getDirtyOps() { return dirtyOps; }
    public long getTotalDirtyBytes() { return totalDirtyBytes; }
    public long getMaxDirtyBytes() { return maxDirtyBytes; }
    /** Average dirty bytes per operation that logged txnBytesDirty; 0 when none did. */
    public long getAvgDirtyBytes() { return dirtyOps > 0 ? totalDirtyBytes / dirtyOps : 0; }
    public long getFlowControlOps() { return flowControlOps; }
    public long getTotalFlowControlMicros() { return totalFlowControlMicros; }
    public long getWriteConcernOps() { return writeConcernOps; }
    public long getTotalWriteConcernMillis() { return totalWriteConcernMillis; }

    /** Share of the given total duration spent waiting for flow control, or null when the duration is 0. */
    public Double flowControlPercentOf(long totalDurationMs) {
        return totalDurationMs > 0 ? 100.0 * totalFlowControlMicros / 1000.0 / totalDurationMs : null;
    }

    /** Share of the given total duration spent waiting for the write concern, or null when the duration is 0. */
    public Double writeConcernPercentOf(long totalDurationMs) {
        return totalDurationMs > 0 ? 100.0 * totalWriteConcernMillis / totalDurationMs : null;
    }
}
