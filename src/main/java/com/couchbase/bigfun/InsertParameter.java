package com.couchbase.bigfun;

public class InsertParameter {
    public long insertIdStart;
    public InsertParameter(long insertIdStart)
    {
        this.insertIdStart = insertIdStart;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof InsertParameter)) {
            return false;
        }
        InsertParameter c = (InsertParameter) o;
        return insertIdStart == c.insertIdStart;
    }
}
