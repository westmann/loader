package com.couchbase.bigfun;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.lang.Thread;
import java.util.function.Consumer;

public class LoadTarget {

    private TargetInfo targetInfo;
    private CouchbaseEnvironment env;
    private Cluster cluster;
    private Bucket bucket;
    private long timeout;

    public LoadTarget(TargetInfo targetInfo) {
        this.targetInfo = targetInfo;
        this.env = DefaultCouchbaseEnvironment.create();
        this.timeout = env.kvTimeout();
        this.cluster = CouchbaseCluster.create(env, targetInfo.host);
        this.bucket = targetInfo.password.equals("") ?
                cluster.openBucket(targetInfo.bucket, targetInfo.password) : cluster.openBucket(targetInfo.bucket);
    }

    public void upsert(JsonDocument doc) {
        Consumer<JsonDocument> op = (x) -> this.upsertWithoutRetry(x);
        retryDocumentOperation(op, doc);
        return;
    }

    public void delete(JsonDocument doc) {
        Consumer<JsonDocument> op = (x) -> this.deleteWithoutRetry(x);
        retryDocumentOperation(op, doc);
        return;
    }

    private void retryDocumentOperation(Consumer<JsonDocument> operation, JsonDocument doc) {
        while (true) {
            try {
                operation.accept(doc);
                break;
            } catch (RuntimeException e) {
                if (e instanceof TemporaryFailureException || e.getCause() instanceof TimeoutException) {
                    System.out.println("+++ caught " + e.toString() + " +++");
                    try {
                        Thread.sleep(this.timeout);
                    }
                    catch (InterruptedException ie) {
                        System.err.println(e.toString());
                        System.exit(-1);
                    }
                    System.out.println("+++ new timeout " + timeout + " +++");
                } else {
                    throw e;
                }
            }
        }
    }

    private void upsertWithoutRetry(JsonDocument doc) {
        this.bucket.upsert(doc, PersistTo.NONE, timeout, TimeUnit.MILLISECONDS);
    }

    private void deleteWithoutRetry(JsonDocument doc) {
        this.bucket.remove(doc, PersistTo.NONE, timeout, TimeUnit.MILLISECONDS);
    }

    public void close() {
        bucket.close();
        cluster.disconnect();
    }
}
