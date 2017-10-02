package com.couchbase.bigfun;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.TemporaryFailureException;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.google.gson.Gson;

import java.lang.annotation.Target;
import java.util.Date;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.util.concurrent.TimeoutException;

/**
 * Unit test for simple App.
 */
public class LoaderTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public LoaderTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( LoaderTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testGson() {
        Book book = new Book("a", "b1", "b2", "title", 100);
        Gson gson = new Gson();
        String bookjson = gson.toJson(book);
        Book book2 = gson.fromJson(bookjson, Book.class);
        boolean r = book.equals(book2);
        assertTrue( r );
    }

    private void testOneModeLoadParameter(MixModeLoadParameter p, Date date) {
        assertTrue(p.intervalMS == 6);
        assertTrue(p.durationSeconds == 7);
        assertTrue(p.startTime.equals(date));
        assertTrue(p.insertPropotion == 8);
        assertTrue(p.updatePropotion == 9);
        assertTrue(p.deletePropotion == 10);
        assertTrue(p.ttlPropotion == 11);
        assertTrue(p.dataInfo.dataFilePath.equals("datafilepath"));
        assertTrue(p.dataInfo.metaFilePath.equals("metafilepath"));
        assertTrue(p.dataInfo.keyFieldName.equals("keyfieldname"));
        assertTrue(p.dataInfo.docsToLoad == 1);
        assertTrue(p.targetInfo.host.equals("host"));
        assertTrue(p.targetInfo.bucket.equals("bucket"));
        assertTrue(p.targetInfo.username.equals("username"));
        assertTrue(p.targetInfo.password.equals("password"));
        assertTrue(p.insertParameter.insertIdStart == 2);
        assertTrue(p.deleteParameter.maxDeleteIds == 3);
        assertTrue(p.ttlParameter.expiryStart == 4);
        assertTrue(p.ttlParameter.expiryEnd == 5);
    }

    public void testMixModeLoadParameters() {
        DataInfo dataInfo = new DataInfo("datafilepath", "metafilepath",  "keyfieldname", 1);
        TargetInfo targetInfo = new TargetInfo("host", "bucket", "username", "password");
        MixModeInsertParameter insertParameter = new MixModeInsertParameter(2);
        MixModeDeleteParameter deleteParameter = new MixModeDeleteParameter(3);
        MixModeTTLParameter ttlParameter = new MixModeTTLParameter(4, 5);
        Date date = new Date((new Date()).getTime() / 1000 * 1000);
        MixModeLoadParameter mixModeLoadParameter = new MixModeLoadParameter(6, 7, date,
                8, 9, 10, 11,
                dataInfo, targetInfo, insertParameter, deleteParameter, ttlParameter);
        MixModeLoadParameters mixModeLoadParameters = new MixModeLoadParameters();
        mixModeLoadParameters.loadParameters.add(mixModeLoadParameter);
        mixModeLoadParameters.loadParameters.add(mixModeLoadParameter);
        Gson gson = new Gson();
        String loadParamsString = gson.toJson(mixModeLoadParameters);
        MixModeLoadParameters mixModeLoadParameters2 = gson.fromJson(loadParamsString, MixModeLoadParameters.class);
        assertTrue(mixModeLoadParameters.loadParameters.size() == 2);
        MixModeLoadParameter mixModeLoadParameter2 = mixModeLoadParameters2.loadParameters.get(0);
        testOneModeLoadParameter(mixModeLoadParameter2, date);
        MixModeLoadParameter mixModeLoadParameter3 = mixModeLoadParameters2.loadParameters.get(1);
        testOneModeLoadParameter(mixModeLoadParameter3, date);
        assertTrue(mixModeLoadParameters.equals(mixModeLoadParameters2));
    }

    private void testOneBatchModeLoadParameter(BatchModeLoadParameter batchModeLoadParameter, String updateoperation, String updateFieldName,
                                               String updatefieldtype, String updateValueStart, String updateValueEnd,
                                               String updateValueFormat, String updateValuesFilePath) {
        assertTrue(batchModeLoadParameter.operation.equals(updateoperation));
        assertTrue(batchModeLoadParameter.ttlParameter.expiryStart == 5);
        assertTrue(batchModeLoadParameter.ttlParameter.expiryEnd == 6);
        assertTrue(batchModeLoadParameter.updateParameter.updateFieldName.equals(updateFieldName));
        assertTrue(batchModeLoadParameter.updateParameter.updateFieldType.equals(updatefieldtype));
        assertTrue(batchModeLoadParameter.updateParameter.updateValueStart.equals(updateValueStart));
        assertTrue(batchModeLoadParameter.updateParameter.updateValueEnd.equals(updateValueEnd));
        assertTrue(batchModeLoadParameter.updateParameter.updateValueFormat.equals(updateValueFormat));
        assertTrue(batchModeLoadParameter.updateParameter.updateValuesFilePath.equals(updateValuesFilePath));
    }

    public void testBatchModeLoadParameters() {
        DataInfo dataInfo = new DataInfo("datafilepath", "metafilepath",  "keyfieldname", 1);
        TargetInfo targetInfo = new TargetInfo("host", "bucket", "username", "password");
        Date date = new Date((new Date()).getTime() / 1000 * 1000);
        BatchModeUpdateParameter updateParameter1 = new BatchModeUpdateParameter("updatefieldname", "integer",
                "1", "2");
        BatchModeUpdateParameter updateParameter2 = new BatchModeUpdateParameter("updatefieldname", "string",
                "updatevaluesfilepath");
        BatchModeUpdateParameter updateParameter3 = new BatchModeUpdateParameter("updatefieldname", "string",
                "updatevalueformat", "3", "4");
        BatchModeTTLParameter ttlParameter = new BatchModeTTLParameter(5, 6);
        BatchModeLoadParameter batchModeLoadParameter1 = new BatchModeLoadParameter(dataInfo, targetInfo, "insert", updateParameter1, ttlParameter);
        BatchModeLoadParameter batchModeLoadParameter2 = new BatchModeLoadParameter(dataInfo, targetInfo, "update", updateParameter2, ttlParameter);
        BatchModeLoadParameter batchModeLoadParameter3 = new BatchModeLoadParameter(dataInfo, targetInfo, "delete", updateParameter3, ttlParameter);
        BatchModeLoadParameters batchModeLoadParameters = new BatchModeLoadParameters();
        batchModeLoadParameters.loadParameters.add(batchModeLoadParameter1);
        batchModeLoadParameters.loadParameters.add(batchModeLoadParameter2);
        batchModeLoadParameters.loadParameters.add(batchModeLoadParameter3);
        Gson gson = new Gson();
        String loadParamsString = gson.toJson(batchModeLoadParameters);
        BatchModeLoadParameters batchModeLoadParameters2 = gson.fromJson(loadParamsString, BatchModeLoadParameters.class);
        assertTrue(batchModeLoadParameters2.loadParameters.size() == 3);
        BatchModeLoadParameter batchModeLoadParameter4 = batchModeLoadParameters2.loadParameters.get(0);
        testOneBatchModeLoadParameter(batchModeLoadParameter4, "insert", "updatefieldname", "integer",
                "1", "2", "", "");
        BatchModeLoadParameter batchModeLoadParameter5 = batchModeLoadParameters2.loadParameters.get(1);
        testOneBatchModeLoadParameter(batchModeLoadParameter5, "update", "updatefieldname", "string",
                "", "", "", "updatevaluesfilepath");
        BatchModeLoadParameter batchModeLoadParameter6 = batchModeLoadParameters2.loadParameters.get(2);
        testOneBatchModeLoadParameter(batchModeLoadParameter6, "delete", "updatefieldname", "string",
                "3", "4", "updatevalueformat", "");
        assertTrue(batchModeLoadParameters.equals(batchModeLoadParameters2));
    }

    public void testRandomCategoryGen()
    {
        {
            String categories[] = {"a", "b"};
            int propotions[] = {1000, 0};
            RandomCategoryGen randomCategoryGen = new RandomCategoryGen(categories, propotions);
            for (int i = 0; i < 100; i++) {
                assertTrue(randomCategoryGen.nextCategory().equals("a"));
            }
        }
        {
            String categories[] = {"a", "b"};
            int propotions[] = {0, 10};
            RandomCategoryGen randomCategoryGen = new RandomCategoryGen(categories, propotions);
            for (int i = 0; i < 100; i++) {
                assertTrue(randomCategoryGen.nextCategory().equals("b"));
            }
        }
        {
            String categories[] = {"a", "b", "c"};
            int propotions[] = {30, 30, 30};
            RandomCategoryGen randomCategoryGen = new RandomCategoryGen(categories, propotions);
            int ca = 0;
            int cb = 0;
            int cc = 0;
            int checknumber = 9000;
            for (int i = 0; i < checknumber; i++) {
                String category = randomCategoryGen.nextCategory();
                if (category.equals("a"))
                    ca++;
                else if (category.equals("b"))
                    cb++;
                else if (category.equals("c"))
                    cc++;
                else
                    assertTrue(false);
            }
            double pca = ((double)ca / (checknumber / 3));
            double pcb = ((double)cb / (checknumber / 3));
            double pcc = ((double)cc / (checknumber / 3));
            assertTrue(pca > 0.9 && pca < 1.1);
            assertTrue(pcb > 0.9 && pcb < 1.1);
            assertTrue(pcc > 0.9 && pcc < 1.1);
        }
    }

    public void testJsonObjectUpdater() {
        BatchModeUpdateParameter updateParameter1 = new BatchModeUpdateParameter("updatefieldname", "integer",
                "100000000001", "100000000005");
        BatchModeUpdateParameter updateParameter2 = new BatchModeUpdateParameter("updatefieldname", "float",
                "100000000001.0", "100000000005.0");
        BatchModeUpdateParameter updateParameter3 = new BatchModeUpdateParameter("updatefieldname", "date",
                "2015-01-02", "2015-02-02");
        BatchModeUpdateParameter updateParameter4 = new BatchModeUpdateParameter("updatefieldname", "time",
                "2015-01-02T00:00:00", "2015-02-02T00:00:00");
        String updatevaluefilepath = "updatevaluesfilepath.txt";
        try {
            File file = new File(updatevaluefilepath);
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            writer.write("teststr1\nteststr2\n");
            writer.close();
        }
        catch (IOException e)
        {
            assertTrue(false);
        }
        BatchModeUpdateParameter updateParameter5 = new BatchModeUpdateParameter("updatefieldname", "string",
                updatevaluefilepath);
        BatchModeUpdateParameter updateParameter6 = new BatchModeUpdateParameter("updatefieldname", "string",
                "test%012d", "100000000001", "100000000005");

        int testnumber = 1000;
        for (int i = 0; i < testnumber; i ++) {
            JsonObjectUpdater ou1 = new JsonObjectUpdater(updateParameter1);
            JsonObject obj = JsonObject.fromJson("{\"updatefieldname\" : 10, \"field2\" : \"abcd\"}");
            ou1.updateJsonObject(obj);
            Object v = obj.get("updatefieldname");
            assertTrue(v instanceof Long);
            assertTrue((long)v >= 100000000001l && (long) v < 100000000005l);
        }
        for (int i = 0; i < testnumber; i ++) {
            JsonObjectUpdater ou1 = new JsonObjectUpdater(updateParameter2);
            JsonObject obj = JsonObject.fromJson("{\"updatefieldname\" : 10.0, \"field2\" : \"abcd\"}");
            ou1.updateJsonObject(obj);
            Object v = obj.get("updatefieldname");
            assertTrue(v instanceof Double);
            assertTrue((double)v >= 100000000001.0 && (double) v < 100000000005.0);
        }
        for (int i = 0; i < testnumber; i ++) {
            JsonObjectUpdater ou1 = new JsonObjectUpdater(updateParameter3);
            JsonObject obj = JsonObject.fromJson("{\"updatefieldname\" : \"2000-01-02\", \"field2\" : \"abcd\"}");
            ou1.updateJsonObject(obj);
            Object v = obj.get("updatefieldname");
            assertTrue(v instanceof String);
            assertTrue(((String)v).compareTo("2015-01-02") >= 0 && ((String) v).compareTo("2015-02-02") < 0);
        }
        for (int i = 0; i < testnumber; i ++) {
            JsonObjectUpdater ou1 = new JsonObjectUpdater(updateParameter4);
            JsonObject obj = JsonObject.fromJson("{\"updatefieldname\" : \"2000-01-02T00:01:02\", \"field2\" : \"abcd\"}");
            ou1.updateJsonObject(obj);
            Object v = obj.get("updatefieldname");
            assertTrue(v instanceof String);
            assertTrue(((String)v).compareTo("2015-01-02T00:00:00") >= 0 && ((String) v).compareTo("2015-02-02T00:00:00") < 0);
        }
        for (int i = 0; i < testnumber; i ++) {
            JsonObjectUpdater ou1 = new JsonObjectUpdater(updateParameter5);
            JsonObject obj = JsonObject.fromJson("{\"updatefieldname\" : \"test0\", \"field2\" : \"abcd\"}");
            ou1.updateJsonObject(obj);
            Object v = obj.get("updatefieldname");
            assertTrue(v instanceof String);
            assertTrue(((String)v).equals("teststr1") || ((String) v).equals("teststr2"));
        }
        for (int i = 0; i < testnumber; i ++) {
            JsonObjectUpdater ou1 = new JsonObjectUpdater(updateParameter6);
            JsonObject obj = JsonObject.fromJson("{\"updatefieldname\" : \"test0\", \"field2\" : \"abcd\"}");
            ou1.updateJsonObject(obj);
            Object v = obj.get("updatefieldname");
            assertTrue(v instanceof String);
            assertTrue(((String)v).compareTo("test100000000001") >= 0 && ((String) v).compareTo("test100000000005") < 0);
        }
    }

    private class LoadTargetTestTimeout extends LoadTarget
    {
        public int retryMax = 2;
        public int currentRetryCount = 0;

        @Override
        protected void upsertWithoutRetry(JsonDocument doc) {
            if (currentRetryCount > retryMax) {
                return;
            }
            else {
                this.currentRetryCount++;
                throw new RuntimeException("test timeout", new TimeoutException("test timeout"));
            }
        }

        @Override
        protected void deleteWithoutRetry(JsonDocument doc) {
            if (currentRetryCount > retryMax) {
                return;
            }
            else {
                this.currentRetryCount++;
                throw new RuntimeException("test timeout");
            }
        }

        public LoadTargetTestTimeout(TargetInfo targetInfo) {
            super(targetInfo);
            this.timeout = this.timeout / 2;
        }
    }

    // Env specific test, disabled by default remove "_" in method to enable
    public void _testLoadTarget()
    {
        TargetInfo targetinfo = new TargetInfo("172.23.98.29", "bucket-1", "bucket-1", "password");
        LoadTarget target = new LoadTarget(targetinfo);
        String key = "1";
        String docJson = "{\"id\" : \"1\", \"updatefieldname\" : \"2000-01-02\", \"field2\" : \"abcd\"}";
        JsonDocument doc = JsonDocument.create(key, JsonObject.fromJson(docJson));
        target.upsert(doc);
        target.delete(doc);
        target.close();
    }

    // Env specific test, disabled by default remove "_" in method to enable
    public void _testLoadTargetRetry() {
        TargetInfo targetinfo = new TargetInfo("172.23.98.29", "bucket-1", "bucket-1", "password");
        String key = "1";
        String docJson = "{\"id\" : \"1\", \"updatefieldname\" : \"2000-01-02\", \"field2\" : \"abcd\"}";
        JsonDocument doc = JsonDocument.create(key, JsonObject.fromJson(docJson));
        {
            LoadTargetTestTimeout target = new LoadTargetTestTimeout(targetinfo);
            target.upsert(doc);
            assertTrue(target.currentRetryCount == 3);
            target.close();
        }
        {
            LoadTargetTestTimeout target = new LoadTargetTestTimeout(targetinfo);
            try {
                target.delete(doc);
                assertTrue(false);
            } catch (Exception e) {
                assertTrue(e instanceof RuntimeException);
                assertTrue(e.getCause() == null);
                assertTrue(target.currentRetryCount == 1);
            }
            target.close();
        }
    }

    private class LoadParameterTest extends LoadParameter {
        public int updateNum;
        public int deleteNum;
        public int insertNum;
        public int ttlNum;
        public LoadParameterTest(DataInfo dataInfo, TargetInfo targetInfo,
                                 int updateNum, int deleteNum,
                                 int insertNum, int ttlNum) {
            super(dataInfo, targetInfo);
            this.updateNum = updateNum;
            this.deleteNum = deleteNum;
            this.insertNum = insertNum;
            this.ttlNum = ttlNum;
        }
    }

    private class LoadDataTest extends LoadData {
        public LoadParameterTest loadParameter;
        public int updateCnt = 0;
        public int deleteCnt = 0;
        public int insertCnt = 0;
        public int ttlCnt = 0;
        private final JsonDocument doc;
        @Override
        public JsonDocument GetNextDocumentForUpdate() {
            if (updateCnt ++ >= loadParameter.updateNum)
                return null;
            else
                return doc;
        }
        @Override
        public JsonDocument GetNextDocumentForDelete() {
            if (deleteCnt ++ >= loadParameter.deleteNum)
                return null;
            else
                return doc;
        }
        @Override
        public JsonDocument GetNextDocumentForInsert() {
            if (insertCnt ++ >= loadParameter.insertNum)
                return null;
            else
                return doc;
        }
        @Override
        public JsonDocument GetNextDocumentTTL() {
            if (ttlCnt ++ >= loadParameter.ttlNum)
                return null;
            else
                return doc;
        }
        public LoadDataTest(DataInfo dataInfo, LoadParameterTest loadParameter) {
            super(dataInfo);
            this.loadParameter = loadParameter;
            doc = JsonDocument.create("1", JsonObject.fromJson("{\"id\" : \"1\"}"));
        }
    }

    private class LoadTargetTest extends LoadTarget
    {
        public int upsertCnt = 0;
        public int deleteCnt = 0;
        @Override
        protected void upsertWithoutRetry(JsonDocument doc) {
            upsertCnt++;
            if (upsertCnt == 1)
                throw new TemporaryFailureException("test loader");
            return;
        }

        @Override
        protected void deleteWithoutRetry(JsonDocument doc) {
            deleteCnt++;
            if (deleteCnt == 1)
                throw new RuntimeException("test loader");
            return;
        }

        public LoadTargetTest(TargetInfo targetInfo) {
            super(targetInfo);
            this.timeout = this.timeout / 2;
        }
    }

    private class LoaderTestClass extends Loader<LoadParameterTest, LoadDataTest> {
        public LoadDataTest getDataForTest() {return getData();}
        public LoadParameterTest getParameterForTest() {return getParameter();}
        @Override
        protected void load() {
            while (true) {
                try {
                    if (!operate("insert"))
                        break;
                } catch (Exception e) {
                    System.err.println(e.toString());
                    continue;
                }
            }
            while (true) {
                try {
                    if (!operate("update"))
                        break;
                } catch (Exception e) {
                    System.err.println(e.toString());
                    continue;
                }
            }
            while (true) {
                try {
                    if (!operate("delete"))
                        break;
                } catch (Exception e) {
                    System.err.println(e.toString());
                    continue;
                }
            }
            while (true) {
                try {
                    if (!operate("ttl"))
                        break;
                } catch (Exception e) {
                    System.err.println(e.toString());
                    continue;
                }
            }
        }

        public LoaderTestClass(LoadParameterTest parameter, LoadTargetTest loadTargetTest) {
            super(parameter, new LoadDataTest(parameter.dataInfo, parameter), (LoadTarget)loadTargetTest);
        }
    }

    public void testLoader() {
        int updateNum = 10;
        int deleteNum = 11;
        int insertNum = 12;
        int ttlNum = 13;
        DataInfo dataInfo = new DataInfo("data.json", "data.meta", "id", 10);
        TargetInfo targetInfo = new TargetInfo("172.23.98.29", "bucket-1", "bucket-1", "password");
        LoadParameterTest loadParam = new LoadParameterTest(dataInfo, targetInfo, updateNum, deleteNum, insertNum, ttlNum);
        LoadTargetTest loadTarget = new LoadTargetTest(targetInfo);
        LoaderTestClass loader = new LoaderTestClass(loadParam, loadTarget);
        loader.start();
        try {
            loader.join();
        }
        catch (Exception e) {
            assertTrue(false);
        }
        assertTrue(loader.getParameterForTest().updateNum == updateNum);
        assertTrue(loader.getParameterForTest().deleteNum == deleteNum);
        assertTrue(loader.getParameterForTest().insertNum == insertNum);
        assertTrue(loader.getParameterForTest().ttlNum == ttlNum);

        assertTrue(loader.getDataForTest().updateCnt == updateNum + 1);
        assertTrue(loader.getDataForTest().insertCnt == insertNum + 1);
        assertTrue(loader.getDataForTest().ttlCnt == ttlNum + 1);
        assertTrue(loader.getDataForTest().deleteCnt == deleteNum + 1);

        assertTrue(loadTarget.deleteCnt == deleteNum);
        assertTrue(loadTarget.upsertCnt == (updateNum + insertNum + ttlNum + 1));

        assertTrue(loader.successStats.deleteNumber == deleteNum - 1);
        assertTrue(loader.failedStat.deleteNumber == 1);
        assertTrue(loader.successStats.insertNumber + loader.successStats.updateNumber + loader.successStats.ttlNumber ==
                insertNum + updateNum + ttlNum);
        assertTrue(loader.failedStat.insertNumber + loader.failedStat.updateNumber + loader.failedStat.ttlNumber ==
                0);
    }
}
