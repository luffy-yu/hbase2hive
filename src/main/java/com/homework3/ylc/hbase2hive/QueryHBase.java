package com.homework3.ylc.hbase2hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.hadoop.hbase.util.Bytes;

import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class QueryHBase {
	
	private final SparkSession spark;
	private final Configuration conf;
	private final JavaSparkContext jsc;
	private static final String hourly_uid_file = "hour_uid.txt";
	private static final String hourly_aid_file = "hour_aid.txt";
	// 写入到uid文件
	public static final String hourly_uid_file_local = "/home/hadoop/" + hourly_uid_file;
	// 写入到aid文件
	public static final String hourly_aid_file_local = "/home/hadoop/" + hourly_aid_file;
	// hdfs
	public static final String hdfs_url = "hdfs://cluster1:9000";
	
	public QueryHBase(String appName)
	{
//    	//in dev mode
//        spark = SparkSession
//        	      .builder()
//        	      .appName("HbaseDemo")
//        	      .master("spark://cluster1:7077")
//        	      .config("spark.testing.memory", "471859200")
//        	      .getOrCreate();
        //in deploy mode
        spark = SparkSession
      	      .builder()
      	      .appName(appName)
      	      .config("fs.defaultFS", "hdfs://cluster1:9000")
      	      .getOrCreate();
        
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", 
        		"172.31.42.237:2181,172.31.43.30:2181,172.31.42.86:2181,172.31.42.215:2181");
 		// start a spark context
 		jsc = new JavaSparkContext(spark.sparkContext());
	}
	public void test()
	{
 	    try {

 	        String tableName = "userbehavior";
 	        conf.set(TableInputFormat.INPUT_TABLE, tableName);
 	        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
// 	        conf.registerKryoClasses(new Class[] {org.apache.hadoop.hbase.client.Result.class});
 	        //conf.registerKryoClasses(new Class[] { org.apache.spark.sql.api.java.StructField.class });
 	        
 	        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(conf,
 	                TableInputFormat.class, ImmutableBytesWritable.class,
 	                Result.class);
 	        System.err.println("Start println count...");
 	        System.err.println(hBaseRDD.count());
 	        System.err.println("End println count...");
 	    } catch (Exception e) {

 	        e.printStackTrace();
 	    }
	}
	
	static PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, UserBehavior> myFunc = 
			new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, UserBehavior>(){
  	   private static final long serialVersionUID = 1L;
  	   public Iterator<Tuple2<Integer, UserBehavior>> call(Tuple2<ImmutableBytesWritable, Result> t)throws Exception {
  		    List<Tuple2<Integer, UserBehavior>> list = new ArrayList<Tuple2<Integer, UserBehavior>>();
  		   // 时间戳的小时数，和原始数据
            String uid = Bytes.toString(t._2.getValue(Bytes.toBytes("Uid"), Bytes.toBytes("")));
            String publish = Bytes.toString(t._2.getValue(Bytes.toBytes("Behavior"), Bytes.toBytes("publish")));                
            String view = Bytes.toString(t._2.getValue(Bytes.toBytes("Behavior"), Bytes.toBytes("view")));
            String comment = Bytes.toString(t._2.getValue(Bytes.toBytes("Behavior"), Bytes.toBytes("comment")));
            String aid = Bytes.toString(t._2.getValue(Bytes.toBytes("Aid"), Bytes.toBytes("")));
            String behaviorTime = Bytes.toString(t._2.getValue(Bytes.toBytes("BehaviorTime"), Bytes.toBytes("")));
            UserBehavior ub = UserBehavior.create(uid, publish, view, comment, aid, behaviorTime);
            Integer timeKey = Util.date2HourTimeStamp(behaviorTime);
            System.err.println(ub.toString());
            // 如果uid或者aid为null则返回key = 0
            if(ub.Aid == null || ub.Uid == null || timeKey == 0)
            {
            	list.add(new Tuple2<Integer, UserBehavior>(0, ub));
            }
            else
            {
            	list.add(new Tuple2<Integer, UserBehavior>(timeKey,ub)); 
            }
            return list.iterator();
	     }
	};
	
    public void upload2hdfs(String filename, String url)
    {
	    //
//	    String url = "hdfs://cluster1:9000";
	    Configuration conf = new Configuration(true);
		conf.set("fs.defaultFS", url);
		conf.setBoolean("dfs.support.append", true);
		
		System.err.println(String.format("Upload local: {%s} to hdfs: {%s}", filename, url + "/"));
		
		try {
			FileSystem fs = FileSystem.get(new URI(url),conf,"hadoop");
			fs.copyFromLocalFile(false, true, new Path(filename), new Path("/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}
    }
	
	public void queryTable(String tableName)
	{
		//clear file content
		UidCount.clearFile(hourly_uid_file_local);
		UidCount.clearFile(hourly_aid_file_local);
 	    try {
 	        conf.set(TableInputFormat.INPUT_TABLE, tableName);
 	        // 设置开始和结束的rowkey
// 	        conf.set(TableInputFormat.SCAN_ROW_START, "1528499424956");
 	        conf.set(TableInputFormat.SCAN_ROW_STOP, "1528500262039");// 前99999条，1528500262039
 	        //999 ： 1528499424956
 	        
 	        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(conf,
 	                TableInputFormat.class, ImmutableBytesWritable.class,
 	                Result.class);
 	        
 	        System.err.println("hBaseRDD count:");
 	        System.err.println(hBaseRDD.count());
 	        System.err.println("hBaseRDD flatMapToPair...");
 	       
            JavaPairRDD<Integer, UserBehavior> hoursRDD = hBaseRDD.flatMapToPair(myFunc);
 	        // 因为直接collect方式，内存会溢出，所以采用其他方式
 	        
 	        JavaRDD<Integer> keys = hoursRDD.keys();
 	        
 	        List<Integer> keysList = keys.collect();
 	        // 去重
 	        List<Object> keysList2 = keysList.stream().distinct().collect(Collectors.toList());
 	        System.err.println("processing all hoursRDD and write result to file...");
 	        System.err.println("Uid file:" + hourly_uid_file_local);
 	        System.err.println("Aid file:" + hourly_aid_file_local);
 	        System.err.println("Total:" + keysList2.size());
 	        int i = 0;
 	        int total = keysList2.size();
 	        for(Object key2: keysList2)
 	        {
 	        	i += 1;
 	        	Integer key = Integer.valueOf(key2.toString());
 	        	System.err.println(" Current:" + i + " / " + total);
 	        	// 过滤掉以 0 为key的数据---异常数据
 	        	if(key == 0)
 	        	{
 	        		continue;
 	        	}
 	        	List<UserBehavior> lookuped = hoursRDD.lookup(key);
 	            List<Tuple2<Integer, UserBehavior>> groupedRDD = new ArrayList<Tuple2<Integer, UserBehavior>>();
 	            for(UserBehavior ub: lookuped)
 	            {
 	            	groupedRDD.add(new Tuple2<Integer, UserBehavior>(key, ub));
 	            }
 	            UidCount.run(jsc, groupedRDD, hourly_uid_file_local, hourly_aid_file_local);
 	        }
 	        System.err.println("upload files...");
 	        upload2hdfs(hourly_uid_file_local, hdfs_url);
 	        upload2hdfs(hourly_aid_file_local, hdfs_url);
 	        System.err.println("load result file to hive...");
 	        OperateHive oh = new OperateHive(jsc);
 	        System.err.println("load uid file...");
 	        oh.loadInto_userbehaviorhour_uid(hdfs_url, hourly_uid_file);
 	        System.err.println("load aid file...");
 	        oh.loadInto_userbehaviorhour_aid(hdfs_url, hourly_aid_file);
 	        
            System.err.println("Finish hbase2hive.");
            
 	    } catch (Exception e) {

 	        e.printStackTrace();
 	    }
	}
	
	public void stop()
	{
		if(jsc != null)
		{
			jsc.stop();
		}
		if(spark != null)
		{
			spark.stop();
		}
	}
}
