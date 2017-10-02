package com.couchbase.bigfun;

public class MixModeInsertParameter {
    public long insertIdStart;
    public MixModeInsertParameter(long insertIdStart)
    {
        this.insertIdStart = insertIdStart;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof MixModeInsertParameter)) {
            return false;
        }
        MixModeInsertParameter c = (MixModeInsertParameter) o;
        return insertIdStart == c.insertIdStart;
    }
}
