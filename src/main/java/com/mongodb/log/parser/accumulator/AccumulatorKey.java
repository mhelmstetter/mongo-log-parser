package com.mongodb.log.parser.accumulator;

import java.io.File;

import com.mongodb.log.parser.Namespace;

public class AccumulatorKey {

    private File file;
    //private String namespace;
    private String dbName;
    private String collName;
    private String command;
    private String appName;
    
    public AccumulatorKey(File file, String dbName, String collName, String command) {
        this(file, dbName, collName, command, null);
    }

    public AccumulatorKey(File file, String dbName, String collName, String command, String appName) {
        this.file = file;
        this.dbName = dbName;
        this.collName = collName;
        this.command = command;
        this.appName = appName;
    }

    public AccumulatorKey(File file, Namespace namespace, String command) {
        this(file, namespace, command, null);
    }

    public AccumulatorKey(File file, Namespace namespace, String command, String appName) {
        this.file = file;
        this.dbName = namespace.getDatabaseName();
        this.collName = namespace.getCollectionName();
        this.command = command;
        this.appName = appName;
    }
    
    public String toString() {
        return hashCode()+"";
    }
    
    public String getNamespace() {
        if (dbName != null && collName != null) {
            return dbName + "." + collName;
        } else if (dbName != null) {
            return dbName;
        } else {
            return "unknown";
        }
    }
    

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((collName == null) ? 0 : collName.hashCode());
        result = prime * result + ((command == null) ? 0 : command.hashCode());
        result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
        result = prime * result + ((file == null) ? 0 : file.hashCode());
        result = prime * result + ((appName == null) ? 0 : appName.hashCode());
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
        AccumulatorKey other = (AccumulatorKey) obj;
        if (collName == null) {
            if (other.collName != null)
                return false;
        } else if (!collName.equals(other.collName))
            return false;
        if (command == null) {
            if (other.command != null)
                return false;
        } else if (!command.equals(other.command))
            return false;
        if (dbName == null) {
            if (other.dbName != null)
                return false;
        } else if (!dbName.equals(other.dbName))
            return false;
        if (file == null) {
            if (other.file != null)
                return false;
        } else if (!file.equals(other.file))
            return false;
        if (appName == null) {
            if (other.appName != null)
                return false;
        } else if (!appName.equals(other.appName))
            return false;
        return true;
    }
    
    

}
