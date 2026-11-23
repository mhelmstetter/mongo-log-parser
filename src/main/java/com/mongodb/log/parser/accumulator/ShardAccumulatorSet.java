package com.mongodb.log.parser.accumulator;

/**
 * The per-shard accumulators a parser task writes to in addition to the global ones.
 */
public class ShardAccumulatorSet {

    public final Accumulator accumulator;
    public final PlanCacheAccumulator planCacheAccumulator;
    public final QueryHashAccumulator queryHashAccumulator;
    public final ErrorCodeAccumulator errorCodeAccumulator;
    public final TransactionAccumulator transactionAccumulator;
    public final IndexStatsAccumulator indexStatsAccumulator;

    public ShardAccumulatorSet(Accumulator accumulator, PlanCacheAccumulator planCacheAccumulator,
            QueryHashAccumulator queryHashAccumulator, ErrorCodeAccumulator errorCodeAccumulator,
            TransactionAccumulator transactionAccumulator, IndexStatsAccumulator indexStatsAccumulator) {
        this.accumulator = accumulator;
        this.planCacheAccumulator = planCacheAccumulator;
        this.queryHashAccumulator = queryHashAccumulator;
        this.errorCodeAccumulator = errorCodeAccumulator;
        this.transactionAccumulator = transactionAccumulator;
        this.indexStatsAccumulator = indexStatsAccumulator;
    }
}
