/**
 * Copyright 2016 Couchbase Inc.
 */
package com.couchbase.bigfun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class Loader {

    static int RECORDS_PER_DOT = 1000;
    static int DOTS_PER_LINE = 80;

    static Set<String> ID_NAMES = new HashSet<>();

    String host = "localhost";
    String bucketname = "default";
    boolean verbose = false;
    String username = null;
    String password = null;

    Loader(String host, String bucketname, String username, String password) {
        this.host = host;
        this.bucketname = bucketname;
        this.username = username;
        this.password = password;
    }

    void load(String filename, long limit, boolean flushBeforeLoad) throws IOException {
        final File file = new File(filename);
        if (!file.exists()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        Cluster cluster = CouchbaseCluster.create(host);

        if (username != null && password != null) {
            createBucket(cluster);
        }
        try {
            Bucket bucket = password != null ? cluster.openBucket(bucketname, password) : cluster.openBucket(bucketname);
            if (flushBeforeLoad && !bucket.bucketManager().flush()) {
                throw new IOException("Could not flush " + bucketname);
            }
            parse(file, limit, bucket);
            if (!bucket.close()) {
                throw new IOException("Could not close " + bucketname);
            }
        } finally {
            cluster.disconnect();
        }
    }

    private void createBucket(Cluster cluster) {
        ClusterManager cm;
        try {
            cm = cluster.clusterManager(username, password);
        } catch (RuntimeException | Error e) {
            throw new RuntimeException("could not access cluster manager", e);
        }
        int quotaMB = 200;
        if (!cm.hasBucket(bucketname)) {
            try {
                cm.insertBucket(new BucketSettings() {
                    @Override
                    public String name() {
                        return bucketname;
                    }

                    @Override
                    public BucketType type() {
                        return BucketType.COUCHBASE;
                    }

                    @Override
                    public int quota() {
                        return quotaMB;
                    }

                    @Override
                    public int port() {
                        return 0;
                    }

                    @Override
                    public String password() {
                        return password;
                    }

                    @Override
                    public int replicas() {
                        return 0;
                    }

                    @Override
                    public boolean indexReplicas() {
                        return false;
                    }

                    @Override
                    public boolean enableFlush() {
                        return true;
                    }
                });
            } catch (RuntimeException | Error e) {
                throw new RuntimeException("could not create bucket " + bucketname, e);
            }
        }
    }

    void parse(File file, long limit, Bucket bucket) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            long count = 0;
            for (String line; (line = br.readLine()) != null; ) {
                if (++count > limit) {
                    break;
                }
                JsonObject obj = JsonObject.fromJson(line);
                boolean upserted = false;
                for (String idName : ID_NAMES) {
                    if (!upserted && obj.containsKey(idName)) {
                        String id = String.valueOf(obj.get(idName));
                        JsonDocument doc = bucket.upsert(JsonDocument.create(id, obj), PersistTo.MASTER);
                        upserted = true;
                        if (verbose) {
                            System.out.println("Upserted " + doc);
                        } else {
                            progress(count, '.');
                        }
                    }
                }
                if (!upserted) {
                    if (verbose) {
                        System.out.println("No key found in " + obj);
                    } else {
                        progress(count, '-');
                    }
                }
            }
            System.out.println();
        }
    }

    private void progress(long count, char c) {
        if (count % RECORDS_PER_DOT == 0) {
            System.out.print(c);
            if (count % (RECORDS_PER_DOT * DOTS_PER_LINE) == 0) {
                System.out.println();
            }
        }
    }

    private static void usage(String msg) {
        String usage = msg + "\nParameters: [options] <filename>\n" + "Options:\n"
                + "  -l <num>        (limit - number of records to load)\n"
                + "  -b <bucketname> (default: \"default\")\n" + "  -f              (flush bucket before loading)\n"
                + "  -h <host>       (default: \"localhost\")\n"
                + "  -k <fieldname>  (key field, can occur more than once, first match is chosen)\n"
                + "  -u <username>   (admin user)\n"
                + "  -p <password>   (admin password)\n";
        System.err.println(usage);
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            usage("no arguments");
        }

        long limit = Long.MAX_VALUE;
        String bucket = "default";
        String host = "localhost";
        String filename = "";
        boolean flushBeforeLoad = false;
        String username = null;
        String password = null;

        int i = 0;
        while (i < args.length) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                if (i + 1 >= args.length) {
                    usage("missing value for option " + arg);
                }
                switch (arg) {
                    case "-l":
                        limit = Long.valueOf(args[++i]);
                        break;
                    case "-b":
                        bucket = args[++i];
                        break;
                    case "-h":
                        host = args[++i];
                        break;
                    case "-k":
                        ID_NAMES.add(args[++i]);
                        break;
                    case "-f":
                        flushBeforeLoad = true;
                        break;
                    case "-u":
                        username = args[++i];
                        break;
                    case "-p":
                        password = args[++i];
                        break;
                    default:
                        usage("unknown option " + arg);
                }
            } else {
                if (!filename.equals("")) {
                    usage("more than 1 filename given");
                }
                filename = arg;
            }
            ++i;
        }
        if (ID_NAMES.isEmpty()) {
            usage("need at least 1 key field");
        }
        if (filename.equals("")) {
            usage("no filename given");
        }

        Loader loader = new Loader(host, bucket, username, password);
        loader.load(filename, limit, flushBeforeLoad);
    }
}
