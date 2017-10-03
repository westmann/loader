package com.couchbase.bigfun;

public class BatchModeLoadParametersGeneratorEntry {
    private static void Usage() {
        System.out.println("BatchModeLoadParametersGeneratorEntry");
        System.exit(1);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            Usage();
        }
    }
}
