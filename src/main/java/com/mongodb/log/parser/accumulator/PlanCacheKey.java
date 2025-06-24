package com.mongodb.log.parser.accumulator;

import com.mongodb.log.parser.Namespace;
import com.mongodb.log.parser.OpType;

/**
 * Key class for aggregating by namespace, operation, queryHash, and planSummary
 */
public class PlanCacheKey {
    
    private final Namespace namespace;
    private final OpType opType;
    private final String queryHash;
    private final String planSummary;
    
    public PlanCacheKey(Namespace namespace, OpType opType, String queryHash, String planSummary) {
        this.namespace = namespace;
        this.opType = opType;
        this.queryHash = queryHash;
        this.planSummary = planSummary != null ? planSummary : "UNKNOWN";
    }
    
    public Namespace getNamespace() {
        return namespace;
    }
    
    public OpType getOpType() {
        return opType;
    }
    
    public String getQueryHash() {
        return queryHash;
    }
    
    public String getPlanSummary() {
        return planSummary;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
        result = prime * result + ((opType == null) ? 0 : opType.hashCode());
        result = prime * result + ((queryHash == null) ? 0 : queryHash.hashCode());
        result = prime * result + ((planSummary == null) ? 0 : planSummary.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PlanCacheKey other = (PlanCacheKey) obj;
        if (namespace == null) {
            if (other.namespace != null)
                return false;
        } else if (!namespace.equals(other.namespace))
            return false;
        if (opType == null) {
            if (other.opType != null)
                return false;
        } else if (!opType.equals(other.opType))
            return false;
        if (queryHash == null) {
            if (other.queryHash != null)
                return false;
        } else if (!queryHash.equals(other.queryHash))
            return false;
        if (planSummary == null) {
            if (other.planSummary != null)
                return false;
        } else if (!planSummary.equals(other.planSummary))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("%s|%s|%s|%s", 
            namespace != null ? namespace.toString() : "null",
            opType != null ? opType.getType() : "null",
            queryHash != null ? queryHash : "null", 
            planSummary);
    }
}