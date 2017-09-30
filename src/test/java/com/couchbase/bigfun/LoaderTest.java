package com.couchbase.bigfun;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.util.Date;

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
    public void testLoader()
    {
        assertTrue( true );
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

    public void testPartitionLoadParameter() {
        TargetInfo target = new TargetInfo("h", "bucket", "user", "pwd");
        DataInfo data = new DataInfo(
                "path",
                "meta",
                "keyfield",
                100);
        InsertParameter insertParameter = new InsertParameter(10000);
        DeleteParameter deleteParameter = new DeleteParameter(100);
        TTLParameter ttlParameter = new TTLParameter(3, 10);
        PartitionLoadParameter plp = new PartitionLoadParameter(
                1,
                100,
                new Date(0),
                100,
                0,
                0,
                0,
                data,
                target,
                insertParameter,
                deleteParameter,
                ttlParameter);
        LoadParameters lp = new LoadParameters();
        lp.partitionLoadParameters.add(plp);
        lp.partitionLoadParameters.add(plp);
        Gson gson = new Gson();
        String lpjson = gson.toJson(lp);
        LoadParameters lp2 = gson.fromJson(lpjson, LoadParameters.class);
        boolean r = lp.equals(lp2);
        assertTrue(r);
    }
}
