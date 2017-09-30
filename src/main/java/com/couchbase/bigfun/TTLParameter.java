package com.couchbase.bigfun;

public class TTLParameter {
    public int expiryStart;
    public int expiryEnd;
    public TTLParameter(int expiryStart, int expiryEnd)
    {
        this.expiryStart = expiryStart;
        this.expiryEnd = expiryEnd;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TTLParameter)) {
            return false;
        }
        TTLParameter c = (TTLParameter) o;
        return expiryStart == c.expiryStart &&
                expiryEnd == c.expiryEnd;
    }
}
