package com.couchbase.bigfun;

import com.couchbase.client.java.document.JsonDocument;

public abstract class LoadData {
    protected DataInfo dataInfo;
    public abstract JsonDocument GetNextDocumentForUpdate();
    public abstract JsonDocument GetNextDocumentForDelete();
    public abstract JsonDocument GetNextDocumentForInsert();
    public abstract JsonDocument GetNextDocumentTTL();
    public LoadData(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
    }
}
