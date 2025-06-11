package com.mongodb.log.parser.accumulator;

import com.mongodb.log.parser.Namespace;

/**
 * Key class for aggregating by queryHash, namespace, and operation
 */
public class QueryHashKey {
    
    private final String queryHash;
    private final Namespace namespace;
    private final String operation;
    
    public QueryHashKey(String queryHash, Namespace namespace, String operation) {
        this.queryHash = queryHash;
        this.namespace = namespace;
        this.operation = operation;
    }
    
    public String getQueryHash() {
        return queryHash;
    }
    
    public Namespace getNamespace() {
        return namespace;
    }
    
    public String getOperation() {
        return operation;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((queryHash == null) ? 0 : queryHash.hashCode());
        result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
        result = prime * result + ((operation == null) ? 0 : operation.hashCode());
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
        QueryHashKey other = (QueryHashKey) obj;
        if (queryHash == null) {
            if (other.queryHash != null)
                return false;
        } else if (!queryHash.equals(other.queryHash))
            return false;
        if (namespace == null) {
            if (other.namespace != null)
                return false;
        } else if (!namespace.equals(other.namespace))
            return false;
        if (operation == null) {
            if (other.operation != null)
                return false;
        } else if (!operation.equals(other.operation))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("%s|%s|%s", 
            queryHash != null ? queryHash : "null",
            namespace != null ? namespace.toString() : "null",
            operation != null ? operation : "null");
    }
}