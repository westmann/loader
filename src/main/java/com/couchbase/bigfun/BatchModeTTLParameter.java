package com.couchbase.bigfun;

public class BatchModeTTLParameter {
    public int expiryStart;
    public int expiryEnd;
    public BatchModeTTLParameter(int expiryStart, int expiryEnd)
    {
        this.expiryStart = expiryStart;
        this.expiryEnd = expiryEnd;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof BatchModeTTLParameter)) {
            return false;
        }
        BatchModeTTLParameter c = (BatchModeTTLParameter) o;
        return expiryStart == c.expiryStart &&
                expiryEnd == c.expiryEnd;
    }
}
