package com.couchbase.bigfun;

public class LoadStats {
    public long insertNumber = 0;
    public long deleteNumber = 0;
    public long updateNumber = 0;
    public long ttlNumber = 0;
    public long insertLatency = 0;
    public long deleteLatency = 0;
    public long updateLatency = 0;
    public long ttlLatency = 0;
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LoadStats)) {
            return false;
        }
        LoadStats c = (LoadStats) o;
        return (insertNumber == c.insertNumber) &&
                (deleteNumber == c.deleteNumber) &&
                (updateNumber == c.updateNumber) &&
                (ttlNumber == c.ttlNumber) &&
                (insertLatency == c.insertLatency) &&
                (deleteLatency == c.deleteLatency) &&
                (updateLatency == c.updateLatency) &&
                (ttlLatency == c.ttlLatency);
    }
}
