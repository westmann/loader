package com.couchbase.bigfun;

public class MixModeLoadParametersGeneratorEntry {
    private static void Usage() {
        System.out.println("MixModeLoadParametersGeneratorEntry");
        System.exit(1);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            Usage();
        }
    }
}
