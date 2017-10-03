/**
 * Copyright 2016 Couchbase Inc.
 */
package com.couchbase.bigfun;

import com.google.gson.Gson;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class MixModeLoaderEntry {
    private static void Usage() {
        System.out.println("MixModeLoaderEntry <batch mode load parameter file name>");
        System.exit(1);
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            Usage();
        }

        String loadParamFile = args[0];
        Gson gson = new Gson();
        MixModeLoadParameters loadParameters;
        try {
            String content = new Scanner(new File(loadParamFile)).useDelimiter("\\Z").next();
            loadParameters = gson.fromJson(content, MixModeLoadParameters.class);
        }
        catch (FileNotFoundException e)
        {
            throw new IllegalArgumentException("Invalid load parameter file " + loadParamFile, e);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Can not parse load parameter file " + loadParamFile, e);
        }

        MixModeLoader loaders[] = new MixModeLoader[loadParameters.loadParameters.size()];
        for (int i = 0; i < loaders.length; i++) {
            loaders[i] = new MixModeLoader(loadParameters.loadParameters.get(i));
        }
        for (int i = 0; i < loaders.length; i++) {
            loaders[i].start();
        }
        long totalDuration = 0;
        LoadStats successStats = new LoadStats();
        LoadStats failedStats = new LoadStats();

        for (int i = 0; i < loaders.length; i++) {
            try {
                loaders[i].join();
                totalDuration += loaders[i].duration;
                successStats.add(loaders[i].successStats);
                failedStats.add(loaders[i].failedStats);
            }
            catch (Exception e)
            {
                System.err.println(e);
                System.exit(1);
            }
        }
        System.out.println(String.format("totalDuration=%d", totalDuration));
        System.out.println(successStats.toString());
        System.out.println(failedStats.toString());
        return;
    }
}
