package com.mongodb.logparse;

// Add this new class for collecting statistics from tasks
class ProcessingStats {
    public final long parseErrors;
    public final long noAttr;
    public final long noCommand;
    public final long noNs;
    public final long foundOps;
    
    public ProcessingStats(long parseErrors, long noAttr, long noCommand, long noNs, long foundOps) {
        this.parseErrors = parseErrors;
        this.noAttr = noAttr;
        this.noCommand = noCommand;
        this.noNs = noNs;
        this.foundOps = foundOps;
    }
}