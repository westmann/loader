package com.couchbase.bigfun;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.io.*;
import java.util.Random;

public class BatchModeLoadData extends LoadData {

    private BatchModeTTLParameter ttlParam;
    private BatchModeUpdateParameter updateParam;

    BufferedReader bufferedReader;
    String keyFieldName;
    long maxDocumentsToLoad;
    long loadedDocuments;

    private int expiryStart;
    private int expiryEnd;

    private JsonObjectUpdater updater;

    private Random random = new Random();

    private JsonDocument GetNextDocument() {
        JsonDocument result = null;
        try {
            if (loadedDocuments < this.maxDocumentsToLoad) {
                String line = bufferedReader.readLine();
                if (line != null) {
                    JsonObject obj = JsonObject.fromJson(line);
                    String id = String.valueOf(obj.get(this.keyFieldName));
                    result = JsonDocument.create(id, obj);
                    loadedDocuments++;
                }
            }
        }
        catch (IOException e) {
            result = null;
        }
        return result;
    }

    private int getRandomExpiry() {
        return this.expiryStart + random.nextInt(this.expiryEnd - this.expiryStart);
    }

    @Override
    public JsonDocument GetNextDocumentForUpdate() {
        JsonDocument doc = GetNextDocument();
        updater.updateJsonObject(doc.content());
        return doc;
    }

    @Override
    public JsonDocument GetNextDocumentForDelete() {
        return GetNextDocument();
    }

    @Override
    public JsonDocument GetNextDocumentForInsert() {
        return GetNextDocument();
    }

    @Override
    public JsonDocument GetNextDocumentTTL() {
        JsonDocument doc = GetNextDocument();
        int expiry = getRandomExpiry();
        return JsonDocument.create(doc.id(), expiry, doc.content());
    }

    public BatchModeLoadData(DataInfo dataInfo, BatchModeTTLParameter ttlParam, BatchModeUpdateParameter updateParam) {
        super(dataInfo);
        this.ttlParam = ttlParam;
        this.updateParam = updateParam;
        File file = new File(dataInfo.dataFilePath);
        try {
            this.bufferedReader = new BufferedReader(new FileReader(file));
        }
        catch (FileNotFoundException e) {
            throw new IllegalArgumentException("File not found " + dataInfo.dataFilePath);
        }
        this.keyFieldName = dataInfo.keyFieldName;
        this.maxDocumentsToLoad = dataInfo.docsToLoad;
        this.loadedDocuments = 0;
        this.expiryStart = ttlParam.expiryStart;
        this.expiryEnd = ttlParam.expiryEnd;
        this.updater = new JsonObjectUpdater(this.updateParam);
    }
}
