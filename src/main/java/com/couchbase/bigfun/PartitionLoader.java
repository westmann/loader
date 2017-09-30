package com.couchbase.bigfun;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.lang.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PartitionLoader extends Thread {

    private static final String INSERT_OPERATION = "insert";
    private static final String DELETE_OPERATION = "delete";
    private static final String UPDATE_OPERATION = "update";
    private static final String TTL_OPERATION = "ttl";

    private PartitionLoadParameter partitionLoadParameter;

    private PartitionLoadData data;

    private PartitionLoadTarget target;

    private RandomCategoryGen operationRandomizer;

    public PartitionLoadStats successStats;

    public PartitionLoadStats failedStat;

    private void sleepTillDueTime(Date dueTime)
    {
        Date currentDate = new Date();
        long msTillDueTime = dueTime.getTime() - currentDate.getTime();
        if (msTillDueTime > 0)
        {
            try {
                Thread.sleep(msTillDueTime);
            }
            catch (InterruptedException e) {
                System.err.println(e.toString());
                System.exit(-1);
            }
        }
    }

    public void run() {

        Date currentOperationDueTime;
        if (this.partitionLoadParameter.startTime.getTime() == 0 || this.partitionLoadParameter.startTime.getTime() < (new Date()).getTime())
            currentOperationDueTime = new Date();
        else
            currentOperationDueTime = this.partitionLoadParameter.startTime;

        sleepTillDueTime(currentOperationDueTime);

        Date operationEndTime = new Date(currentOperationDueTime.getTime() + this.partitionLoadParameter.durationSeconds * 1000);

        while (true) {

            if ((new Date()).getTime() >= operationEndTime.getTime())
                break;

            if (this.partitionLoadParameter.intervalMS != 0) {
                sleepTillDueTime(currentOperationDueTime);
                currentOperationDueTime = new Date(currentOperationDueTime.getTime() + this.partitionLoadParameter.intervalMS);
            }

            try {
                String operation = operationRandomizer.nextCategory();
                switch (operation) {
                    case INSERT_OPERATION:
                        this.insert();
                        break;
                    case DELETE_OPERATION:
                        this.delete();
                        break;
                    case UPDATE_OPERATION:
                        this.update();
                        break;
                    case TTL_OPERATION:
                        this.ttl();
                        break;
                    default:
                        break;
                }
            }
            catch (Exception e)
            {
                System.err.println(e.toString());
                continue;
            }
        }
    }

    private void ttl() throws IOException, InterruptedException {
        try {
            JsonDocument doc = data.GetNextDocumentTTL();
            this.target.upsert(doc);
            this.successStats.ttlNumber++;
        }
        catch (Exception e) {
            this.failedStat.ttlNumber++;
            throw e;
        }
        return;
    }

    private void insert() {
        try {
            JsonDocument doc = data.GetNextDocumentForInsert();
            this.target.upsert(doc);
            this.successStats.insertNumber++;
        }
        catch (Exception e) {
            this.failedStat.insertNumber++;
            throw e;
        }
        return;
    }

    private void delete() {
        try {
            JsonDocument doc = data.GetNextDocumentForDelete();
            this.target.delete(doc);
            this.successStats.deleteNumber++;
        }
        catch (Exception e) {
            this.failedStat.deleteNumber++;
            throw e;
        }
        return;
    }

    private void update() {
        try {
            JsonDocument doc = data.GetNextDocumentForUpdate();
            this.target.upsert(doc);
            this.successStats.updateNumber++;
        }
        catch (Exception e) {
            this.failedStat.updateNumber++;
            throw e;
        }
        return;
    }

    public PartitionLoader(PartitionLoadParameter partitionLoadParameter) {
        this.partitionLoadParameter = partitionLoadParameter;

        this.data = new PartitionLoadData(this.partitionLoadParameter.dataInfo, this.partitionLoadParameter.insertParameter,
                this.partitionLoadParameter.deleteParameter, this.partitionLoadParameter.ttlParameter);

        this.target = new PartitionLoadTarget(this.partitionLoadParameter.targetInfo);

        String operations[] = {
                INSERT_OPERATION,
                DELETE_OPERATION,
                UPDATE_OPERATION,
                TTL_OPERATION};
        int operationPropotions[] = {
                this.partitionLoadParameter.insertPropotion,
                this.partitionLoadParameter.deletePropotion,
                this.partitionLoadParameter.updatePropotion,
                this.partitionLoadParameter.ttlPropotion};
        this.operationRandomizer = new RandomCategoryGen(operations, operationPropotions);
    }
}
