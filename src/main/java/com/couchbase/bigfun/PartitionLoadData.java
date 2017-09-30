package com.couchbase.bigfun;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.io.*;

import java.util.Random;

import java.io.BufferedReader;

import java.util.HashSet;

public class PartitionLoadData {

    private DataInfo dataInfo;

    private InsertParameter insertParam;
    private DeleteParameter delParam;
    private TTLParameter ttlParameter;

    private long operationIdStart;
    private long operationIdEnd;

    private long extraInsertIdStart;
    private long currentExtraInsertId;

    private JsonDocument templateDocuments[];

    private long deletedDocNumberThreshold;

    private int expiryStart;
    private int expiryEnd;

    private HashSet<String> removedKeys = new HashSet<String>();

    private Random random = new Random();

    private String getRandomNonRemovedKey() {
        while (true) {
            String key = String.valueOf(operationIdStart + (long) (random.nextDouble() * (operationIdEnd - operationIdStart)));
            if (removedKeys.contains(key)) {
                continue;
            }
            return key;
        }
    }

    private String getRandomKeyToInsert() {
        if (removedKeys.size() > this.deletedDocNumberThreshold) {
            String k = this.removedKeys.iterator().next();
            removedKeys.remove(k);
            return k;
        }
        else
            return String.valueOf(currentExtraInsertId++);
    }

    private String getRandomKeyToRemove() {
        String k = getRandomNonRemovedKey();
        removedKeys.add(k);
        return k;
    }

    private int getRandomExpiry() {
        return this.expiryStart + random.nextInt(this.expiryEnd - this.expiryStart);
    }

    private JsonDocument getRandomDocumentTemplate() {
        int docIdx = random.nextInt(templateDocuments.length);
        return this.templateDocuments[docIdx];
    }

    public JsonDocument GetNextDocumentForUpdate() {
        String keyToUpdate = getRandomNonRemovedKey();
        JsonDocument docTemplate = getRandomDocumentTemplate();
        return JsonDocument.create(keyToUpdate, docTemplate.content());
    }

    public JsonDocument GetNextDocumentForDelete() {
        String keyToDelete = getRandomKeyToRemove();
        JsonDocument docTemplate = getRandomDocumentTemplate();
        return JsonDocument.create(keyToDelete, docTemplate.content());
    }

    public JsonDocument GetNextDocumentForInsert() {
        String keyToInsert = getRandomKeyToInsert();
        JsonDocument docTemplate = getRandomDocumentTemplate();
        return JsonDocument.create(keyToInsert, docTemplate.content());
    }

    public JsonDocument GetNextDocumentTTL() {
        String keyToTTL = getRandomKeyToRemove();
        JsonDocument docTemplate = getRandomDocumentTemplate();
        int expiry = getRandomExpiry();
        return JsonDocument.create(keyToTTL, expiry, docTemplate.content());
    }

    public PartitionLoadData(DataInfo dataInfo, InsertParameter insertParam,
                             DeleteParameter delParam, TTLParameter ttlParameter) {
        this.dataInfo = dataInfo;
        this.insertParam = insertParam;
        this.delParam = delParam;
        this.ttlParameter = ttlParameter;

        this.extraInsertIdStart = this.insertParam.insertIdStart;
        this.currentExtraInsertId = extraInsertIdStart;

        this.deletedDocNumberThreshold = this.delParam.maxDeleteIds;

        try {
            BufferedReader br = new BufferedReader(new FileReader(this.dataInfo.metaFilePath));
            String line = br.readLine();
            this.operationIdStart = 0;
            this.operationIdEnd = 0;
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Invalid meta file path");
        }

        try {
            this.templateDocuments = new JsonDocument[dataInfo.docsReadInMem];
            BufferedReader br = new BufferedReader(new FileReader(this.dataInfo.dataFilePath));
            for (int i = 0; i < this.dataInfo.docsReadInMem; i++) {
                String line = br.readLine();
                if (line == null)
                    break;
                JsonObject obj = JsonObject.fromJson(line);
                String id = String.valueOf(obj.get(this.dataInfo.keyFieldName));
                this.templateDocuments[i] = JsonDocument.create(id, obj);
            }
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Invalid data file path");
        }
    }
}
