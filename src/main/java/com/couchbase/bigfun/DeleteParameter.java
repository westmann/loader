package com.couchbase.bigfun;

public class DeleteParameter {
    public long maxDeleteIds;
    public DeleteParameter(long maxDeleteIds)
    {
        this.maxDeleteIds = maxDeleteIds;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DeleteParameter)) {
            return false;
        }
        DeleteParameter c = (DeleteParameter) o;
        return maxDeleteIds == c.maxDeleteIds;
    }
}
