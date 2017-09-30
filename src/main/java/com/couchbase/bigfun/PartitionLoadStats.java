package com.couchbase.bigfun;

public class PartitionLoadStats {
    public long insertNumber = 0;
    public long deleteNumber = 0;
    public long updateNumber = 0;
    public long ttlNumber = 0;
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof PartitionLoadStats)) {
            return false;
        }
        PartitionLoadStats c = (PartitionLoadStats) o;
        return (insertNumber == c.insertNumber) &&
                (deleteNumber == c.deleteNumber) &&
                (updateNumber == c.updateNumber) &&
                (ttlNumber == c.ttlNumber);
    }
}
