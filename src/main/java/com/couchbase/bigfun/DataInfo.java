package com.couchbase.bigfun;

public class DataInfo {
    public String dataFilePath;
    public String metaFilePath;
    public String keyFieldName;
    public int docsReadInMem;

    public DataInfo(String dataFilePath, String metaFilePath, String keyFieldName, int docsReadInMem){
        this.dataFilePath = dataFilePath;
        this.metaFilePath = metaFilePath;
        this.keyFieldName = keyFieldName;
        this.docsReadInMem = docsReadInMem;
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
                (docsReadInMem == c.docsReadInMem);
    }
}
