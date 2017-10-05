package com.couchbase.bigfun;

public class MixModeDeleteParameter {
    public long maxDeleteIds;
    public MixModeDeleteParameter(long maxDeleteIds)
    {
        this.maxDeleteIds = maxDeleteIds;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof MixModeDeleteParameter)) {
            return false;
        }
        MixModeDeleteParameter c = (MixModeDeleteParameter) o;
        return maxDeleteIds == c.maxDeleteIds;
    }
}
