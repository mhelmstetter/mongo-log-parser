package com.mongodb.log.parser.accumulator;

import com.mongodb.log.parser.Namespace;

/**
 * Key class for aggregating by planCacheKey, queryHash, and planSummary
 */
public class PlanCacheKey {
    
    private final Namespace namespace;
    private final String planCacheKey;
    private final String queryHash;
    private final String planSummary;
    
    public PlanCacheKey(Namespace namespace, String planCacheKey, String queryHash, String planSummary) {
        this.namespace = namespace;
        this.planCacheKey = planCacheKey;
        this.queryHash = queryHash;
        this.planSummary = planSummary != null ? planSummary : "UNKNOWN";
    }
    
    public Namespace getNamespace() {
        return namespace;
    }
    
    public String getPlanCacheKey() {
        return planCacheKey;
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
        result = prime * result + ((planCacheKey == null) ? 0 : planCacheKey.hashCode());
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
        if (planCacheKey == null) {
            if (other.planCacheKey != null)
                return false;
        } else if (!planCacheKey.equals(other.planCacheKey))
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
            planCacheKey != null ? planCacheKey : "null",
            queryHash != null ? queryHash : "null", 
            planSummary);
    }
}