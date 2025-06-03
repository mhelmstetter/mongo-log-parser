package com.mongodb.logparse;


public enum OpType {
    CMD(1, "command", true, false),
    QUERY(2, "find", true, true),
    GETMORE(3, "getmore", true, false), // FIXME does this actually support query shape?
    INSERT(4, "insert", false, false),  // FIXME do we want to compute a query shape for the object inserted?
    UPDATE(5, "update", true, true),
    UPDATE_W(6, "update_w", true, true),
    REMOVE(7, "remove", true, false),
	AGGREGATE(8, "aggregate", true, true),
	FIND_AND_MODIFY(9, "findAndModify", true, true),
	DISTINCT(10, "distinct", true, true),
	COUNT(10, "COUNT", true, true);

    OpType(final int pCode, final String pOpType, final boolean pSupportsQueryShape, final boolean pSupportsExecStats) {
        this.code = pCode;
        this.type = pOpType;
        this.supportsQueryShape = pSupportsQueryShape;
        this.supportsExecStats = pSupportsExecStats;
    }

    public final int code;
    private final String type;
    private final boolean supportsQueryShape;
    private final boolean supportsExecStats;

    public String getType() {
        return type;
    }

    public boolean hasQueryShape() {
        return this.supportsQueryShape;
    }

    public boolean hasExecStats() {
        return this.supportsExecStats;
    }
    
    public static OpType findByType(final String pOpType) {
        final String lowerOpType = pOpType.toLowerCase();

        if (lowerOpType.equals("command")) {
          return OpType.CMD;
        } else if (lowerOpType.equals("query")) {
          return OpType.QUERY;
        } else if (lowerOpType.equals("getmore")) {
          return OpType.GETMORE;
        } else if (lowerOpType.equals("insert")) {
          return OpType.INSERT;
        } else if (lowerOpType.equals("update")) {
          return OpType.UPDATE;
        } else if (lowerOpType.equals("remove")) {
          return OpType.REMOVE;
        }

        return null;
      }

    public static OpType findByCode(final int pCode) {
        switch(pCode) {
        case 1:
            return OpType.CMD;
        case 2:
            return OpType.QUERY;
        case 3:
            return OpType.GETMORE;
        case 4:
            return OpType.INSERT;
        case 5:
            return OpType.UPDATE;
        case 6:
            return OpType.REMOVE;
        default:
            throw new IllegalArgumentException("Unknown OpType: _code=" + pCode);
        }
    }

    public static OpType findByType(final String opType, final String pOpType) {
        final String lowerOpType = pOpType.toLowerCase();

        if (lowerOpType.equals("command")) {
            return OpType.CMD;
        } else if (lowerOpType.equals("query")) {
            return OpType.QUERY;
        } else if (lowerOpType.equals("find")) {
            return OpType.QUERY;
        } else if (lowerOpType.equals("getmore")) {
            return OpType.GETMORE;
        } else if (lowerOpType.equals("insert")) {
            return OpType.INSERT;
        } else if (lowerOpType.equals("update")) {
            if (opType.equals("WRITE")) {
                return OpType.UPDATE_W;
            } else if (opType.equals("COMMAND")) {
                return OpType.UPDATE;
            } else {
                throw new IllegalArgumentException("Unknown op opType: " +opType);
            }
            
        } else if (lowerOpType.equals("remove")) {
            return OpType.REMOVE;
        }

        return null;
    }
}
