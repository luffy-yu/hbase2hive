package com.homework3.ylc.hbase2hive;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {
	
	public final static String DateFormat = "yyyy-MM-dd HH:mm:ss";
	
	public static Integer date2HourTimeStamp(String date){  
	    try {  
	        SimpleDateFormat sdf = new SimpleDateFormat(DateFormat);  
	        return (int) (sdf.parse(date).getTime() /1000 / 3600 * 3600);  
	    } catch (Exception e) {  
	        e.printStackTrace();  
	    }  
	    return 0;  
	}
	
	public static Integer date2HourTimeStamp_hour(String date)
	{
		long second = date2HourTimeStamp(date);
		String ts = timeStamp2Date(second);
		String hour = ts.substring(11, 13);
		return Integer.valueOf(hour);
	}
	
	public static String date2HourTimeStamp_day(String date)
	{
		long second = date2HourTimeStamp(date);
		String ts = timeStamp2Date(second);
		String day = ts.substring(0, 10);
		return day;
	}
	
	public static String timeStamp2Date(long seconds) {  
	    if(seconds == 0){  
	        return "";  
	    }   
	    SimpleDateFormat sdf = new SimpleDateFormat(DateFormat);  
	    return sdf.format(new Date(Long.valueOf(seconds * 1000)));  
	}
	
	public static void test()
	{
    	String date = " 2015-01-09  17:03:41";
		System.out.println(date);
    	int timeStamp = (int)date2HourTimeStamp(date);  
    	System.out.println(timeStamp);
    	System.out.println("hour");
    	System.out.println(date2HourTimeStamp_hour(date));
    	System.out.println("day");
    	System.out.println(date2HourTimeStamp_day(date));
    	String date2 = timeStamp2Date(timeStamp); 
    	System.out.println(date2);
	}
	
	public static void main(String []argv)
	{
		test();
	}
}
