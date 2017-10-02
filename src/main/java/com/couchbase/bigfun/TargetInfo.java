package com.couchbase.bigfun;

public class TargetInfo {
    public String host;
    public String bucket;
    public String username;
    public String password;
    public TargetInfo(String host, String bucket, String username, String password)
    {
        this.host = host;
        this.bucket = bucket;
        this.username = username;
        this.password = password;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TargetInfo)) {
            return false;
        }
        TargetInfo c = (TargetInfo) o;
        return host.equals(c.host) &&
        bucket.equals(c.bucket) &&
        username.equals(c.username) &&
        password.equals(c.password);
    }
}
