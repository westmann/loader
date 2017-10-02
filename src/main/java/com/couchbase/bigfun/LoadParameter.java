package com.couchbase.bigfun;

public class LoadParameter {
    public DataInfo dataInfo;
    public TargetInfo targetInfo;
    public LoadParameter(DataInfo dataInfo, TargetInfo targetInfo) {
        this.dataInfo = dataInfo;
        this.targetInfo = targetInfo;
    }
    @Override
    public boolean equals(Object o){
        if (o == this) {
            return true;
        }
        if (!(o instanceof LoadParameter)) {
            return false;
        }
        LoadParameter c = (LoadParameter) o;
        return dataInfo.equals(c.dataInfo) &&
                targetInfo.equals(c.targetInfo);
    }
}
