package com.couchbase.bigfun;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.lang.Thread;
import java.util.function.Consumer;
import java.nio.charset.Charset;
import java.io.UnsupportedEncodingException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

public class LoadTarget {

    private TargetInfo targetInfo;
    private CouchbaseEnvironment env;
    private Cluster cluster;
    private Bucket bucket;
    protected long timeout;
    private HttpClient cbasClient;

    public LoadTarget(TargetInfo targetInfo) {
        this.targetInfo = targetInfo;
        this.env = DefaultCouchbaseEnvironment.create();
        this.timeout = env.kvTimeout();
        this.cluster = CouchbaseCluster.create(env, targetInfo.host);
        this.bucket = targetInfo.password.equals("") ?
                cluster.openBucket(targetInfo.bucket) : cluster.openBucket(targetInfo.bucket, targetInfo.password);
        this.cbasClient = HttpClientBuilder.create().build();
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

    public JsonObject cbasQuery(String query) {
        JsonObject result = null;
        if (!this.targetInfo.cbashost.equals("")) {
            HttpPost post = createCbasPost();
            String data = createCbasQueryJson(query);
            try {
                post.setEntity(new StringEntity(data));
                HttpResponse response = cbasClient.execute(post);
                BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                StringBuffer stringBuffer = new StringBuffer();
                String line;
                while ((line = reader.readLine()) != null) {
                    stringBuffer.append(line);
                }
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    result = JsonObject.fromJson(stringBuffer.toString());
                }
            } catch (Exception e) {
                System.out.println("exception occured while sending http request : " + e);
            } finally {
                post.releaseConnection();
            }
        }
        return result;
    }

    private String createCbasQueryJson(String query) {
        JsonObject queryObj = JsonObject.create();
        queryObj.put("statement", query);
        queryObj.put("pretty", true);
        return queryObj.toString();
    }

    private HttpPost createCbasPost() {
        String restUrl = String.format("http://%s:8095/analytics/service", this.targetInfo.cbashost);
        HttpPost post = new HttpPost(restUrl);
        String auth=new StringBuffer(this.targetInfo.username).append(":").append(this.targetInfo.password).toString();
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);
        post.setHeader("AUTHORIZATION", authHeader);
        post.setHeader("Content-Type", "application/json");
        post.setHeader("Accept", "application/json");
        post.setHeader("X-Stream" , "true");
        return post;
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

    protected void upsertWithoutRetry(JsonDocument doc) {
        this.bucket.upsert(doc, PersistTo.NONE, timeout, TimeUnit.MILLISECONDS);
    }

    protected void deleteWithoutRetry(JsonDocument doc) {
        this.bucket.remove(doc, PersistTo.NONE, timeout, TimeUnit.MILLISECONDS);
    }

    public void close() {
        bucket.close();
        cluster.disconnect();
    }
}
