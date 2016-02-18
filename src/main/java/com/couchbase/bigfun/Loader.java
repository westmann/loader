/**
 * Copyright 2016 Couchbase Inc.
 */
package com.couchbase.bigfun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class Loader {

    static String ID = "id";
    static String MESSAGE_ID = "message-id";
    static String TWEETID = "tweetid";
    static String SCREEN_NAME = "screen-name";

    static int RECORDS_PER_DOT = 1000;
    static int DOTS_PER_LINE = 80;

    static String[] ID_NAMES = new String[] { ID, MESSAGE_ID, TWEETID, SCREEN_NAME };
    String host = "localhost";
    String bucketname = "default";
    boolean verbose = false;

    Loader(String host, String bucketname) {
        this.host = host;
        this.bucketname = bucketname;
    }

    void load(String filename, long limit) throws IOException {
        final File file = new File(filename);
        if (!file.exists()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        Cluster cluster = CouchbaseCluster.create(host);
        try {
            Bucket bucket = cluster.openBucket(bucketname);
            parse(file, limit, bucket);
            if (!bucket.close()) {
                throw new IOException("Could not close " + bucketname);
            }
        } finally {
            cluster.disconnect();
        }
    }

    void parse(File file, long limit, Bucket bucket) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            long count = 0;
            for (String line; (line = br.readLine()) != null;) {
                if (++count > limit) {
                    break;
                }
                JsonObject obj = JsonObject.fromJson(line);
                for (String idName : ID_NAMES) {
                    if (obj.containsKey(idName)) {
                        String id = (idName == SCREEN_NAME ? obj.getString(idName)
                                : Long.toString(obj.getLong(idName)));
                        JsonDocument doc = bucket.upsert(JsonDocument.create(id, obj));
                        if (verbose) {
                            System.out.println("Upserted " + doc);
                        } else {
                            if (count % RECORDS_PER_DOT == 0) {
                                System.out.print('.');
                                if (count % (RECORDS_PER_DOT * DOTS_PER_LINE) == 0) {
                                    System.out.println();
                                }
                            }
                        }
                    }
                }
            }
            System.out.println();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Parameters: <filename> (<limit> (<bucketname> (<host>)?)?)?");
            System.exit(1);
        }
        String filename = args[0]; // "/tmp/data/sg/fb_message.adm";
        long limit = (args.length > 1 ? Long.valueOf(args[1]) : Long.MAX_VALUE);
        String bucket = (args.length > 2 ? args[2] : "default");
        String host = (args.length > 3 ? args[3] : "localhost");

        Loader loader = new Loader(host, bucket);
        loader.load(filename, limit);
    }
}
