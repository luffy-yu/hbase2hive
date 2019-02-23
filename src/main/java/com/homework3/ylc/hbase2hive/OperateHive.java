package com.homework3.ylc.hbase2hive;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.*;

public class OperateHive {
	
	private final JavaSparkContext _jsc;
	private final HiveContext hiveContext;
	public OperateHive(JavaSparkContext jsc)
	{
		_jsc = jsc;
		hiveContext = new org.apache.spark.sql.hive.HiveContext(_jsc.sc());
	}
	
	/**
	 * 加载数据到userbehaviorhour_uid表
	 * 表的定义语句如下：
	 * create table userbehaviorhour_uid
		(
		uid STRING,
		behavior_total INT,
		behavior_publish INT,
		behavior_view INT,
		behavior_comment INT,
		day_time STRING,
		hour_time INT
		);
	 */
	public void loadInto_userbehaviorhour_uid(String url, String filename)
	{
		hiveContext.sql("DROP TABLE IF EXISTS userbehaviorhour_uid ");
		hiveContext.sql("create table IF NOT EXISTS userbehaviorhour_uid (uid STRING,behavior_total INT,behavior_publish INT,behavior_view INT,behavior_comment INT,day_time STRING,hour_time INT)");
//        String sql = String.format("LOAD DATA LOCAL INPATH '%s/%s' INTO TABLE userbehaviorhour_uid", url, filename);
        String sql = String.format("LOAD DATA INPATH '%s/%s' INTO TABLE userbehaviorhour_uid", url, filename);
        hiveContext.sql(sql);
	}
	
	/**
	 * 加载数据到userbehaviorhour_aid表
	 * 表的定义语句如下：
	 * create table userbehaviorhour_aid
		(
		Aid STRING,
		behavior_total INT,
		behavior_publish INT,
		behavior_view INT,
		behavior_comment INT,
		day_time STRING,
		hour_time INT
		);
	 */
	public void loadInto_userbehaviorhour_aid(String url, String filename)
	{
		hiveContext.sql("DROP TABLE IF EXISTS userbehaviorhour_aid ");
		hiveContext.sql("create table IF NOT EXISTS userbehaviorhour_aid (aid STRING,behavior_total INT,behavior_publish INT,behavior_view INT,behavior_comment INT,day_time STRING,hour_time INT)");
//        String sql = String.format("LOAD DATA LOCAL INPATH '%s/%s' INTO TABLE userbehaviorhour_aid", url, filename);
        String sql = String.format("LOAD DATA INPATH '%s/%s' INTO TABLE userbehaviorhour_aid", url, filename);
		hiveContext.sql(sql);
	}
	
	public static void main(String args[])
	{
		
	}
}
