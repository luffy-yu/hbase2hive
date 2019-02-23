package com.homework3.ylc.hbase2hive;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UidCount implements Serializable{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private UserBehavior _userBehavior;
    private int _total;//总数
	private static final String hive_url = "jdbc:hive2://cluster1:10000/default";
	private static final String hive_user = "hive";
	private static final String hive_pwd = "hive";
    public UidCount(UserBehavior user,int total){
        _userBehavior=user;
        _total=total;
    }
    public String toString(){
        return _userBehavior.toString() + " Total Count:" + String.valueOf(_total);
    }
    
    //createCombiner()
    static Function<UserBehavior,UidCount> createCombiner =new Function<UserBehavior,UidCount>(){
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public UidCount call(UserBehavior x){
            return new UidCount(x,1);
        }
    };
    
    //mergeValue()
    static Function2<UidCount,UserBehavior,UidCount> mergeValue_Uid=new Function2<UidCount, UserBehavior, UidCount>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public UidCount call(UidCount a, UserBehavior x) throws Exception {
            a._userBehavior.addByUid(x);
            a._total+=1;
            return a;
        }
    };
    //mergeValue2()
    static Function2<UidCount,UserBehavior,UidCount> mergeValue_Aid=new Function2<UidCount, UserBehavior, UidCount>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public UidCount call(UidCount a, UserBehavior x) throws Exception {
            a._userBehavior.addByAid(x);
            a._total+=1;
            return a;
        }
    }; 
    
    //mmergeCombiners()
    static Function2<UidCount,UidCount,UidCount> mergeCombiner_Uid=new Function2<UidCount, UidCount, UidCount>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public UidCount call(UidCount a, UidCount b) throws Exception {
            a._userBehavior.addByUid(b._userBehavior);;
            a._total+=b._total;
            return a;
        }
    };
    
    //mmergeCombiners2()
    static Function2<UidCount,UidCount,UidCount> mergeCombiner_Aid=new Function2<UidCount, UidCount, UidCount>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public UidCount call(UidCount a, UidCount b) throws Exception {
            a._userBehavior.addByAid(b._userBehavior);;
            a._total+=b._total;
            return a;
        }
    };
    
    /*
     * 将UserBehavior序列转换为以Uid为主键的List,
     * 用于按照用户统计用户的行为。
     * 用Uid作为主键，UserBehavior对象作为值
     */
    public static List<Tuple2<String,UserBehavior>> userBehaviorListByUid(Iterable<UserBehavior> in)
    {
    	List<Tuple2<String,UserBehavior>> ret = new ArrayList<Tuple2<String, UserBehavior>>();
    	
    	for(UserBehavior ub: in)
    	{
    		ret.add(new Tuple2<String, UserBehavior>(ub.Uid, ub));
    	}
    	
    	return ret;
    }
    
    /*
     * 将UserBehavior序列转换为以Aid为主键的List,
     * 用于按照文章统计用户的行为。
     * 用Aid作为主键，UserBehavior对象作为值。
     */
    public static List<Tuple2<String,UserBehavior>> userBehaviorListByAid(Iterable<UserBehavior> in)
    {
    	List<Tuple2<String,UserBehavior>> ret = new ArrayList<Tuple2<String, UserBehavior>>();
    	
    	for(UserBehavior ub: in)
    	{
    		ret.add(new Tuple2<String, UserBehavior>(ub.Aid, ub));
    	}
    	
    	return ret;
    }
    
    public static void clearFile(String filename)
    {
    	FileWriter fw = null;
		try {
			fw = new FileWriter(filename,false);
	    	fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}
    }
    
    public static void write_aid_hour(String filename, UserBehavior ub)
    {
    	try
    	{
//    	    String filename= "target/MyFile.txt";
    		String day = Util.date2HourTimeStamp_day(ub.BehaviorTime);
    		Integer hour = Util.date2HourTimeStamp_hour(ub.BehaviorTime);
    	    FileWriter fw = new FileWriter(filename,true); //append
    	    Integer total = (int)(ub.publish + ub.view + ub.comment);
    	    
    	    String line = String.format("%s\001%d\001%d\001%d\001%d\001%s\001%s\n", 
    	    		ub.Aid,total,ub.publish,ub.view,ub.comment,day,hour);
    	    
    	    fw.write(line);
    	    fw.close();
    	}
    	catch(Exception ioe)
    	{
    	    System.err.println("write_aid_hour Exception: " + ioe.getMessage());
    	    ioe.printStackTrace(System.err);
    	}
    }
    
    public static void write_uid_hour(String filename, UserBehavior ub)
    {
    	try
    	{
    		String day = Util.date2HourTimeStamp_day(ub.BehaviorTime);
    		Integer hour = Util.date2HourTimeStamp_hour(ub.BehaviorTime);
    	    FileWriter fw = new FileWriter(filename,true); //append
    	    Integer total = (int)(ub.publish + ub.view + ub.comment);
    	    
    	    String line = String.format("%s\001%d\001%d\001%d\001%d\001%s\001%s\n", 
    	    		ub.Uid,total,ub.publish,ub.view,ub.comment,day,hour);
    	    fw.write(line);
    	    fw.close();
    	}
    	catch(Exception ioe)
    	{
    	    System.err.println("write_uid_hour Exception: " + ioe.getMessage());
    	    ioe.printStackTrace(System.err);
    	}
    }
    
    public static void run(JavaSparkContext sc, List<Tuple2<Integer,UserBehavior>> list, 
    		String out_uid_file, String out_aid_file)
    {
        JavaPairRDD<Integer,UserBehavior> users= sc.parallelizePairs(list);
    	// 第一步按照时间分类统计用户的行为
        // 按照时间key做group运算
        JavaPairRDD<Integer, Iterable<UserBehavior>> grouped = users.groupByKey();
        // 将结果按照key(时间)-value(UserBehavior集合)的形式聚集在一起
        Map<Integer, Iterable<UserBehavior>> groupedMap = grouped.collectAsMap();
        // 第二步按照时间内的用户的行为
        // 按照key遍历，value为用户的行为集合
        for(Map.Entry<Integer, Iterable<UserBehavior>> entry: groupedMap.entrySet())
        {
        	// 转换为以Uid为主键的List
            List<Tuple2<String,UserBehavior>> uidUsers = userBehaviorListByUid(entry.getValue());
            // 
            JavaPairRDD<String,UserBehavior> parallelized = sc.parallelizePairs(uidUsers);
            // 通过key(Uid)合并
            JavaPairRDD<String,UidCount> Combined=parallelized.combineByKey(createCombiner,mergeValue_Uid,mergeCombiner_Uid);
            // 收集结果
            Map<String,UidCount> countMap= Combined.collectAsMap();
            for(Map.Entry<String,UidCount> entry2:countMap.entrySet())
            {
            	// 将结果是写出hive数据库
//            	System.out.println("Uid Key:"+ entry2.getKey()+ " value:"+entry2.getValue().toString());
            	write_uid_hour(out_uid_file, entry2.getValue()._userBehavior);
            }
            
            //转换为以Aid为主键的List
            List<Tuple2<String,UserBehavior>> aidUsers = userBehaviorListByAid(entry.getValue());
            // 
            JavaPairRDD<String,UserBehavior> parallelized_aid = sc.parallelizePairs(aidUsers);
            // 通过key(Uid)合并
            JavaPairRDD<String,UidCount> combined_aid=parallelized_aid.combineByKey(createCombiner,mergeValue_Aid,mergeCombiner_Aid);
            // 收集结果
            Map<String,UidCount> countMap_aid= combined_aid.collectAsMap();
            for(Map.Entry<String,UidCount> entry2:countMap_aid.entrySet())
            {
            	// 将结果是写出hive数据库
//            	System.out.println("Aid Key:"+ entry2.getKey()+ " value:"+entry2.getValue().toString());
            	write_aid_hour(out_aid_file, entry2.getValue()._userBehavior);
            }
            
        }
    }
    
    public static void test()
    {
    	//in dev mode
        SparkSession spark = SparkSession
      	      .builder()
      	      .appName("AvgCount")
      	      .master("local")
      	      .config("spark.testing.memory", "471859200")
      	      .getOrCreate();
        
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        List<Tuple2<Integer,UserBehavior>> list=new ArrayList<Tuple2<Integer, UserBehavior>>();
        // key为时间，value为原始值
        list.add(new Tuple2<Integer, UserBehavior>(1,UserBehavior.create("1", "1", "0", "0", "A", "0001")));
        list.add(new Tuple2<Integer, UserBehavior>(1,UserBehavior.create("1", "0", "1", "0", "B", "0001")));
        list.add(new Tuple2<Integer, UserBehavior>(1,UserBehavior.create("2", "0", "1", "0", "C", "0002")));
        list.add(new Tuple2<Integer, UserBehavior>(1,UserBehavior.create("2", "0", "0", "1", "A", "0002")));
        list.add(new Tuple2<Integer, UserBehavior>(2,UserBehavior.create("1", "1", "0", "0", "B", "0001")));
        list.add(new Tuple2<Integer, UserBehavior>(2,UserBehavior.create("1", "0", "0", "1", "D", "0001")));
        list.add(new Tuple2<Integer, UserBehavior>(2,UserBehavior.create("1", "0", "0", "1", "A", "0001")));
        
        run(sc, list, QueryHBase.hourly_uid_file_local, QueryHBase.hourly_aid_file_local);
        
        spark.stop();
        sc.close();
    }
    
    public static void main(String args[]){
    	UserBehavior ub = UserBehavior.create("C", "1", "1", "0", "A1", " 2015-01-09  16:03:41");
    	
    	write_aid_hour(QueryHBase.hourly_aid_file_local, ub);
    	write_uid_hour(QueryHBase.hourly_uid_file_local, ub);
    }
}
