package com.couchbase.bigfun;

public class MixModeTTLParameter {
    public int expiryStart;
    public int expiryEnd;
    public MixModeTTLParameter(int expiryStart, int expiryEnd)
    {
        this.expiryStart = expiryStart;
        this.expiryEnd = expiryEnd;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof MixModeTTLParameter)) {
            return false;
        }
        MixModeTTLParameter c = (MixModeTTLParameter) o;
        return expiryStart == c.expiryStart &&
                expiryEnd == c.expiryEnd;
    }
}
