package com.couchbase.bigfun;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.HashMap;
import java.io.File;
import java.io.FilenameFilter;

public class BatchModeLoadParametersGeneratorEntry {
    private static void Usage() {
        System.out.println("BatchModeLoadParametersGeneratorEntry [Option]+");
        System.out.println("Options:");
        System.out.println("-P <partitionpath>");
        System.out.println("-d <datafile>");
        System.out.println("-k <keyfield>");
        System.out.println("-l <doctoload>");
        System.out.println("-h <host>");
        System.out.println("-u <username>");
        System.out.println("-p <password>");
        System.out.println("-b <bucket>");
        System.out.println("-o <operation>");
        System.out.println("-t <ttlstart>:<ttlend>");
        System.out.println("-U <fieldname>:<fieldtype>:<valuestart>:<valueend>");
        System.out.println("-U <fieldname>:<fieldtype>:<valueformat><valuestart>:<valueend>");
        System.out.println("-U <fieldname>:<fieldtype>:<valuesfile>");
        System.exit(1);
    }

    private static void parseTTLArg(HashMap<String,Object> arguments, String arg) {
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

    private static void parseUpdateArg(HashMap<String,Object> arguments, String arg) {
        String parts[] = arg.split("#");
        if (parts.length <= 2)
            throw new IllegalArgumentException("Invalid update parameter " + arg);
        try {
            arguments.put("fieldName", parts[0]);
            arguments.put("fieldType", parts[1]);
            if (parts.length == 3) {
                arguments.put("valuesFile", parts[2]);
            }
            else if (parts.length == 4) {
                arguments.put("valueStart", parts[2]);
                arguments.put("valueEnd", parts[3]);
            }
            else if (parts.length == 5) {
                arguments.put("valueFormat", parts[2]);
                arguments.put("valueStart", parts[3]);
                arguments.put("valueEnd", parts[4]);
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Invalid update parameter " + arg, e);
        }
        return;
    }

    private static HashMap<String,Object> parseArguments(String[] args) {
        HashMap<String,Object> result = new HashMap<String,Object>();
        int i = 0;
        while (i < args.length) {

            String arg = args[i];

            if (arg.startsWith("-")) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for option " + arg);
                }

                switch (arg) {
                    case "-P":
                        result.put("partitionPath", args[++i]);
                        break;
                    case "-d":
                        result.put("dataFile", args[++i]);
                        break;
                    case "-l":
                        result.put("docToLoad", Long.valueOf(args[++i]));
                        break;
                    case "-h":
                        result.put("host", args[++i]);
                        break;
                    case "-u":
                        result.put("user", args[++i]);
                        break;
                    case "-p":
                        result.put("pwd", args[++i]);
                        break;
                    case "-b":
                        result.put("bucket", args[++i]);
                        break;
                    case "-o":
                        result.put("operation", args[++i]);
                        break;
                    case "-t":
                        parseTTLArg(result, args[++i]);
                        break;
                    case "-U":
                        parseUpdateArg(result, args[++i]);
                        break;
                    case "-k":
                        result.put("keyField", args[++i]);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown option " + arg);
                }
            }
            ++i;
        }

        return result;
    }

    private static String[] getAllSubFolders(String partitionPath)
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

    public static void main(String[] args) {
        HashMap<String, Object> arguments = null;
        try {
            arguments = parseArguments(args);
            if (arguments == null || arguments.size() < 1) {
                Usage();
            }
        }
        catch (IllegalArgumentException e)
        {
            System.err.println(e.toString());
            Usage();
        }

        String allPartitions[] = getAllSubFolders((String)arguments.get("partitionPath"));
        TargetInfo targetInfo = new TargetInfo((String)arguments.get("host"), (String)arguments.get("bucket"),
        (String)arguments.get("user"), (String)arguments.get("pwd"));
        BatchModeTTLParameter ttlParameter = new BatchModeTTLParameter((int)arguments.get("ttlStart"), (int)arguments.get("ttlEnd"));
        BatchModeUpdateParameter updateParameter = null;
        if (arguments.containsKey("valueFormat")) {
            updateParameter = new BatchModeUpdateParameter((String) arguments.get("fieldName"), (String) arguments.get("fieldType"),
                    (String) arguments.get("valueFormat"), (String) arguments.get("valueStart"), (String) arguments.get("valueEnd"));
        }
        else if (arguments.containsKey("valuesFile")) {
            updateParameter = new BatchModeUpdateParameter((String) arguments.get("fieldName"), (String) arguments.get("fieldType"),
                    (String) arguments.get("valuesFile"));
        }
        else {
            updateParameter = new BatchModeUpdateParameter((String) arguments.get("fieldName"), (String) arguments.get("fieldType"),
                    (String) arguments.get("valueStart"), (String) arguments.get("valueEnd"));
        }
        BatchModeLoadParameters batchModeLoadParameters = new BatchModeLoadParameters();
        for (int i = 0; i < allPartitions.length; i++) {
            File dataFile = new File(allPartitions[i], (String)arguments.get("dataFile") + ".json");
            File metaFile = new File(allPartitions[i], (String)arguments.get("dataFile") + ".meta");
            if (!dataFile.exists() || !dataFile.isFile())
                throw new IllegalArgumentException("Invalid partition data file " + dataFile.getAbsolutePath());
            if (!metaFile.exists() || !metaFile.isFile())
                throw new IllegalArgumentException("Invalid partition meta file " + metaFile.getAbsolutePath());
            DataInfo dataInfo = new DataInfo(dataFile.getAbsolutePath(), metaFile.getAbsolutePath(),
                    (String)arguments.get("keyField"), (long)arguments.get("docToLoad"));
            BatchModeLoadParameter batchModeLoadParameter = new BatchModeLoadParameter(dataInfo, targetInfo,
                    (String)arguments.get("operation"), updateParameter, ttlParameter);
            batchModeLoadParameters.loadParameters.add(batchModeLoadParameter);
        }
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String parametersJson = gson.toJson(batchModeLoadParameters);
        System.out.println(parametersJson);
        return;
    }
}
