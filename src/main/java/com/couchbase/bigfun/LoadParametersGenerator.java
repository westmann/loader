package com.couchbase.bigfun;

import java.io.File;
import java.io.FilenameFilter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public abstract class LoadParametersGenerator {

    protected HashMap<String,Object> arguments = new HashMap<String,Object>();
    protected String args[];

    public LoadParametersGenerator(String args[]) {
        this.args = args;
    }

    protected abstract void run();

    protected void PrintOptionsUsage() {
        System.out.println("-P <partitionpath>");
        System.out.println("-d <datafile>");
        System.out.println("-k <keyfield>");
        System.out.println("-l <doctoload>");
        System.out.println("-h <host>");
        System.out.println("-u <username>");
        System.out.println("-p <password>");
        System.out.println("-b <bucket>");
    }

    protected void parseArguments() {
        int i = 0;
        while (i < args.length) {

            String arg = args[i];

            if (arg.startsWith("-")) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for option " + arg);
                }

                switch (arg) {
                    case "-P":
                        arguments.put("partitionPath", args[++i]);
                        break;
                    case "-d":
                        arguments.put("dataFile", args[++i]);
                        break;
                    case "-l":
                        arguments.put("docToLoad", Long.valueOf(args[++i]));
                        break;
                    case "-h":
                        arguments.put("host", args[++i]);
                        break;
                    case "-u":
                        arguments.put("user", args[++i]);
                        break;
                    case "-p":
                        arguments.put("pwd", args[++i]);
                        break;
                    case "-b":
                        arguments.put("bucket", args[++i]);
                        break;
                    case "-k":
                        arguments.put("keyField", args[++i]);
                        break;
                }
            }
            ++i;
        }

        return;
    }

    protected static void parseTTLArg(HashMap<String,Object> arguments, String arg) {
        String parts[] = arg.split("#");
        if (parts.length != 2)
            throw new IllegalArgumentException("Invalid TTL parameter " + arg);
        try {
            int ttlStart = Integer.valueOf(parts[0]);
            int ttlEnd = Integer.valueOf(parts[1]);
            arguments.put("ttlStart", ttlStart);
            arguments.put("ttlEnd", ttlEnd);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Invalid TTL parameter " + arg, e);
        }
        return;
    }

    protected static void parseTime(HashMap<String,Object> arguments, String name, String arg) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        try {
            arguments.put(name, formatter.parse(arg));
        }
        catch (ParseException e) {
            throw new IllegalArgumentException("Invalid time argument " + arg, e);
        }
        return;
    }

    protected static String[] getAllSubFolders(String partitionPath)
    {
        File file = new File(partitionPath);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });
        for (int i = 0; i < directories.length; i ++)
        {
            directories[i] = new File(file, directories[i]).getAbsolutePath();
        }
        return directories;
    }
}
