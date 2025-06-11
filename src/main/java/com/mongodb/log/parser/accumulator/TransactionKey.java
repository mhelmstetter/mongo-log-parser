package com.mongodb.log.parser.accumulator;

/**
 * Key class for aggregating transactions by retry counter, termination cause, and commit type
 */
public class TransactionKey {
    
    private final Integer txnRetryCounter;
    private final String terminationCause;
    private final String commitType;
    
    public TransactionKey(Integer txnRetryCounter, String terminationCause, String commitType) {
        this.txnRetryCounter = txnRetryCounter;
        this.terminationCause = terminationCause;
        this.commitType = commitType;
    }
    
    public Integer getTxnRetryCounter() {
        return txnRetryCounter;
    }
    
    public String getTerminationCause() {
        return terminationCause;
    }
    
    public String getCommitType() {
        return commitType;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((txnRetryCounter == null) ? 0 : txnRetryCounter.hashCode());
        result = prime * result + ((terminationCause == null) ? 0 : terminationCause.hashCode());
        result = prime * result + ((commitType == null) ? 0 : commitType.hashCode());
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
        TransactionKey other = (TransactionKey) obj;
        if (txnRetryCounter == null) {
            if (other.txnRetryCounter != null)
                return false;
        } else if (!txnRetryCounter.equals(other.txnRetryCounter))
            return false;
        if (terminationCause == null) {
            if (other.terminationCause != null)
                return false;
        } else if (!terminationCause.equals(other.terminationCause))
            return false;
        if (commitType == null) {
            if (other.commitType != null)
                return false;
        } else if (!commitType.equals(other.commitType))
            return false;
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("%s|%s|%s", 
            txnRetryCounter != null ? txnRetryCounter.toString() : "null",
            terminationCause != null ? terminationCause : "null",
            commitType != null ? commitType : "null");
    }
}
