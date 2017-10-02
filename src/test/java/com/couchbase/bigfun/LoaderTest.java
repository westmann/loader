package com.couchbase.bigfun;

import com.couchbase.client.java.document.json.JsonObject;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.google.gson.Gson;

import java.util.Date;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;

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
}
