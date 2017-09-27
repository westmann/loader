/**
 * Copyright 2016 Couchbase Inc.
 */
package com.couchbase.bigfun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;

public class Loader extends Thread {

    static int RECORDS_PER_DOT = 1000;
    static int DOTS_PER_LINE = 80;

    static Set<String> ID_NAMES = new HashSet<>();

    CouchbaseEnvironment env;
    String filename;
    long start;
    long limit;
    boolean flushBeforeLoad;
    String host = "localhost";
    String bucketname = "default";
    boolean verbose = false;
    String username = null;
    String password = null;
    long timeout;
    Operation operation = null;

    Loader(String filename, long start, long limit, boolean flushBeforeLoad, String host, String bucketname, String username, String password, boolean verbose, Operation op) {
        this.env = DefaultCouchbaseEnvironment.create();
        this.filename = filename;
        this.start = start;
        this.limit = limit;
        this.flushBeforeLoad = flushBeforeLoad;
        this.host = host;
        this.bucketname = bucketname;
        this.verbose = verbose;
        this.username = username;
        this.password = password;
        this.timeout = env.kvTimeout();
        this.operation = op;
    }

    void load() throws IOException, InterruptedException, ParseException {
        System.out.println("Starting loading " + filename + " to " + host + "/" + bucketname);
        final File file = new File(filename);
        if (!file.exists()) {
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        Cluster cluster = CouchbaseCluster.create(env, host);

        if (username != null && password != null) {
            createBucket(cluster);
        }
        try {
            Bucket bucket = password != null ? cluster.openBucket(bucketname, password)
                    : cluster.openBucket(bucketname);
            if (flushBeforeLoad && !bucket.bucketManager().flush()) {
                throw new IOException("Could not flush " + bucketname);
            }
            parse(file, start, limit, bucket);
            if (!bucket.close()) {
                throw new IOException("Could not close " + bucketname);
            }
        } finally {
            cluster.disconnect();
        }
        System.out.println("Finished loading " + filename + " to " + host + "/" + bucketname);
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

    void parse(File file, long start, long limit, Bucket bucket) throws IOException, InterruptedException, ParseException {
        System.out.println("+++ load start +++");
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            long count = 0;
            for (String line; (line = br.readLine()) != null;) {
                if (count++ < start) {
                    continue;
                }
                if (++count > limit) {
                    break;
                }
                JsonObject obj = JsonObject.fromJson(line);
                boolean operationdone = false;
                for (String idName : ID_NAMES) {
                    if (!operationdone && obj.containsKey(idName)) {
                        String id = String.valueOf(obj.get(idName));
                        JsonDocument doc = JsonDocument.create(id, obj);
                        while (!operationdone) {
                            operationdone = operate(bucket, doc);
                        }
                        if (verbose) {
                            System.out.println("Operation done " + doc);
                        } else {
                            progress(count, '.');
                        }
                    }
                }
                if (!operationdone) {
                    if (verbose) {
                        System.out.println("No key found in " + obj);
                    } else {
                        progress(count, '-');
                    }
                }
            }
            System.out.println();
        } finally {
            System.out.println("+++ load end +++");
        }
    }

    private boolean operate(Bucket bucket, JsonDocument doc) throws InterruptedException, ParseException {
        return this.operation.operate(bucket, doc, timeout);
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
                + "  -s <num>        (start - start number of records to load)\n"
		+ "  -l <num>        (limit - number of records to load)\n"
                + "  -b <bucketname> (default: \"default\")\n" 
                + "  -f              (flush bucket before loading)\n"
                + "  -h <host>       (default: \"localhost\")\n"
                + "  -k <fieldname>  (key field, can occur more than once, first match is chosen)\n"
                + "  -u <username>   (admin user)\n" 
                + "  -p <password>   (admin password)\n"
                + "  -v              (verbose)\n"
                + "  -d              (delete documents)\n"
                + "  -m <fieldname>:<type>:<start>:<end> (update field with random value of type from start to end)\n"
                + "  -m <fieldname>:<type>:<value list file> (update field with random value of type from a value list file)\n";

        System.err.println(usage);
        System.exit(1);
    }

    public void run() {
        try {
            this.load();
        }
        catch (Exception e) {
            System.err.println(e.toString());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ParseException {
        if (args.length == 0) {
            usage("no arguments");
        }

        long limit = Long.MAX_VALUE;
        long start = 0;
        String bucket = "default";
        String host = "localhost";
        ArrayList<String> files = new ArrayList<String>();
        boolean flushBeforeLoad = false;
        boolean verbose = false;
        String username = null;
        String password = null;
        Operation op = new Operation("insert", null);

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
                    case "-v":
                        verbose = true;
                        break;
                    case "-u":
                        username = args[++i];
                        break;
                    case "-p":
                        password = args[++i];
                        break;
                    case "-s":
                        start = Long.valueOf(args[++i]);
			            break;
                    case "-d":
                        op = new Operation("delete", null);
                        break;
                    case "-m":
                        op = new Operation("update", args[++i]);
                        break;
                    default:
                        usage("unknown option " + arg);
                }
            } else {
                File folder = new File(arg);
                File[] listOfFiles = folder.listFiles();
                for (int j = 0; j < listOfFiles.length; j++) {
                    if (listOfFiles[j].isFile()) {
                        files.add(listOfFiles[j].getAbsolutePath());
                    }
                }
            }
            ++i;
        }

        if (ID_NAMES.isEmpty()) {
            usage("need at least 1 key field");
        }

        if (files.size() == 0) {
            usage("no filename given");
        }

        ArrayList<Loader> loaders = new ArrayList<Loader>();
        for (int j = 0; j < files.size(); j++) {
            String filename = files.get(j);
            Loader loader = new Loader(filename, start, limit, flushBeforeLoad, host, bucket, username, password, verbose, op);
            loader.start();
            loaders.add(loader);
        }
        for (int j = 0; j < loaders.size(); j++) {
            Loader loader = loaders.get(j);
            loader.join();
        }
    }
}
