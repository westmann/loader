package com.couchbase.bigfun;

public class DataInfo {
    public String dataFilePath;
    public String metaFilePath;
    public String keyFieldName;
    public int docsToLoad;

    public DataInfo(String dataFilePath, String metaFilePath, String keyFieldName, int docsToLoad){
        this.dataFilePath = dataFilePath;
        this.metaFilePath = metaFilePath;
        this.keyFieldName = keyFieldName;
        this.docsToLoad = docsToLoad;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DataInfo)) {
            return false;
        }
        DataInfo c = (DataInfo) o;
        return dataFilePath.equals(c.dataFilePath) &&
                metaFilePath.equals(c.metaFilePath) &&
                keyFieldName.equals(c.keyFieldName) &&
                (docsToLoad == c.docsToLoad);
    }
}
