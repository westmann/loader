package com.couchbase.bigfun;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
}
