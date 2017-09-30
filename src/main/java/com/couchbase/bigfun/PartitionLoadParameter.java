package com.couchbase.bigfun;

import java.lang.annotation.Target;
import java.util.Date;

public class PartitionLoadParameter {
    public int intervalMS;
    public long durationSeconds;
    public Date startTime;
    public int insertPropotion;
    public int updatePropotion;
    public int deletePropotion;
    public int ttlPropotion;
    public DataInfo dataInfo;
    public TargetInfo targetInfo;
    public InsertParameter insertParameter;
    public DeleteParameter deleteParameter;
    public TTLParameter ttlParameter;
    public PartitionLoadParameter(int intervalMS, int durationSeconds, Date startTime,
                                  int insertPropotion, int updatePropotion,
                                  int deletePropotion, int ttlPropotion,
                                  DataInfo dataInfo, TargetInfo targetInfo,
                                  InsertParameter insertParameter, DeleteParameter deleteParameter,
                                  TTLParameter ttlParameter)
    {
        this.intervalMS = intervalMS;
        this.durationSeconds = durationSeconds;
        this.startTime = startTime;
        this.insertPropotion = insertPropotion;
        this.updatePropotion = updatePropotion;
        this.deletePropotion = deletePropotion;
        this.ttlPropotion = ttlPropotion;
        this.dataInfo = dataInfo;
        this.targetInfo = targetInfo;
        this.insertParameter = insertParameter;
        this.deleteParameter = deleteParameter;
        this.ttlParameter = ttlParameter;
    }
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof PartitionLoadParameter)) {
            return false;
        }
        PartitionLoadParameter c = (PartitionLoadParameter) o;
        return (intervalMS == c.intervalMS) &&
                (durationSeconds == c.durationSeconds) &&
                startTime.equals(c.startTime) &&
                (insertPropotion == c.insertPropotion) &&
                (updatePropotion == c.updatePropotion) &&
                (deletePropotion == c.deletePropotion) &&
                (ttlPropotion == c.ttlPropotion) &&
                dataInfo.equals(c.dataInfo) &&
                targetInfo.equals(c.targetInfo) &&
                insertParameter.equals(c.insertParameter) &&
                deleteParameter.equals(c.deleteParameter) &&
                ttlParameter.equals(c.ttlParameter);
    }
}
