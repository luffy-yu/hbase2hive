package com.homework3.ylc.hbase2hive;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//        System.out.println( "Hello World!" );
    	QueryHBase qh = new QueryHBase("QueryHBase");
    	qh.queryTable("userbehavior");
    }
}
