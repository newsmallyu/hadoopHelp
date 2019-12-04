package com.newegg.hadoopHelp.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

import java.io.*;

public class HDFSUtil {
	
	
	private static Logger logger = Logger.getLogger(HDFSUtil.class);

	//private static Configuration conf = new Configuration();

	// 创建目录
	public static void mkdir(String path,Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(path);
		boolean isok = fs.mkdirs(srcPath);
		if (isok) {
			System.out.println("create dir ok!");
		} else {
			System.out.println("create dir failure");
		}
		fs.close();
	}

	// 删除文件
	public static void delete(String filePath,Configuration conf) throws IOException {
		if(filePath.toLowerCase().startsWith("hdfs")){
			conf.set("fs.default.name", filePath.substring(0,filePath.indexOf("/", 10)));
		}
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		boolean isok = fs.deleteOnExit(path);
		if (isok) {
			System.out.println("delete ok!");
		} else {
			System.out.println("delete failure");
		}
		fs.close();
	}
	// 下载文件
	public static void downloadFile(String src, String dest,Configuration conf) throws IOException {
		FileSystem srcFS = FileSystem.get(conf);
		Path srcPath = new Path(src); // 原路径
		Path destPath = new Path(dest); // 原路径


		FileStatus[] fileStatus = srcFS.listStatus(srcPath);
		for (FileStatus file : fileStatus) {
			if(file.isFile()){
				Path path = file.getPath();
				FSDataInputStream in = srcFS.open(path);
				Path destFile = new Path(destPath, path.getName());
				File outFile = new File(destFile.toString());
				if(outFile.exists()) outFile.delete();
				outFile.createNewFile();
				FileOutputStream out = new FileOutputStream(outFile);

				// 缓冲数组
				byte[] b = new byte[1024 * 4];
				int len=0;
				while((len=in.read(b)) != -1){
					out.write(b, 0, len);
				}
				in.close();
				out.close();

				System.out.println("Copy file '"+destFile.getName()+"' finish.");
			}
		}
	}
	// 上传本地文件
	public static void uploadFile(String src, String dst,Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(src); // 原路径
		Path dstPath = new Path(dst); // 目标路径
		// 调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
		fs.copyFromLocalFile(false, srcPath, dstPath);

		// 打印文件路径
		System.out.println("Upload to " + conf.get("fs.default.name"));
		System.out.println("------------list files------------" + "\n");
		FileStatus[] fileStatus = fs.listStatus(dstPath);
		for (FileStatus file : fileStatus) {
			System.out.println(file.getPath());
		}
		fs.close();
	}

	public static String exist(String filePath,Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		if(fs.exists(path)){
			return ("Path ["+filePath+"] exists");
		}else{
			return ("Path ["+filePath+"] is not exists");
		}
		
	}


	public static void main(String[] args) {
		try {
			//System.setProperty("HADOOP_USER_NAME", "ecappservice");		//设置hadoop用户
			//System.setProperty("HADOOP_USER_NAME", "tw79");		//设置hadoop用户
			//System.setProperty("HADOOP_USER_NAME", "azkaban");		//设置hadoop用户
			//System.setProperty("HADOOP_USER_NAME", "nassau");		//设置hadoop用户
			//System.setProperty("HADOOP_USER_NAME", "root");		//设置hadoop用户
			System.setProperty("HADOOP_USER_NAME", "ecappservice");		//设置hadoop用户
			//System.setProperty("HADOOP_USER_NAME", "hbase");		//设置hadoop用户
			//conf.set("fs.default.name", "hdfs://10.16.238.92:8020");//设置连接
			//conf.set("fs.default.name", "hdfs://172.16.131.56:8020");//设置连接
			//conf.set("fs.default.name", "hdfs://172.16.31.132:8020");//PRD
			//conf.set("fs.default.name", "hdfs://172.16.59.132:8020");//PRD
			//conf.set("fs.default.name", "hdfs://172.16.13.142:8020");//GQC
			
			//HDFSMain.writeFile("/etljob/searchpartial_test/startTime","2015-08-08 20:30:00");
			//HDFSMain.readFile("/etljob/searchpartial_test/startTime");   //2015-08-08 21:10:00
			
			//HDFSUtil.delete("hdfs://10.16.238.92:8020/user/reason/wordout");
			//HDFSUtil.readFile("hdfs://10.16.238.92:8020/user/reason/wordout/part-r-00000");
			
			//HDFSUtil.printLog("Reason");
			//HDFSMain.mvDir("/user/reason/test3", "/user/reason/test5");
			//HDFSUtil.mkdir("/user/root/");
			//HDFSUtil.mkdir("/user/ecappservice/etljob/customer_review/reviewfull");
			//HDFSMain.showList("/etljob/searchpartial");
			//HDFSMain.createFile("/user/reason/test/a.txt","reason".getBytes());
			
			//HDFSUtil.delete("hdfs://sxlab19-0:8020/user/hbase/temp/user_test");
			//HDFSUtil.exist("hdfs://10.16.236.126:9000/etljob/customer_review/schema/avro");
			//HDFSUtil.mkdir("hdfs://10.16.236.126:9000/etljob/customer_review/schema/avro");
			
			//HDFSUtil.uploadFile("D:\\ideaWorkSpace\\DemoProject2\\hdfsDemo\\schema\\HBase_EC_ItemReviewBase.avsc", "/etljob/customer_review/schema/avro/HBase_EC_ItemReviewBase.avsc");
			//HDFSUtil.uploadFile("D:\\ideaWorkSpace\\DemoProject2\\hdfsDemo\\schema\\MG_Item_Review_All.avsc", "/etljob/customer_review/schema/avro/MG_Item_Review_All.avsc");
			//HDFSUtil.uploadFile("C:\\Users\\rd87\\Desktop\\Job\\CustomerReview\\schame\\user_test.avsc", "/user/ec/etljob/customer_review/schema/avro/user_test.avsc");
			//HDFSUtil.uploadFile("D:\\GitSpace\\etl-customer-review\\azkabanjob\\customer_review_all\\schema\\MG_Item_Review_All.avsc",
			//		"/etljob/customer_review/schema/avro/MG_Item_Review_All.avsc");
			//		"/user/ecappservice/etljob/customer_review/schema/avro/HBase_EC_ItemReviewBase.avsc");
			//HDFSUtil.uploadFile("D:\\aTemp\\temperature.txt", "/user/rd87/test/temperature.txt");
			//HDFSUtil.uploadFile("D:\\aTemp\\WishList\\users.txt", "/user/hbase/source/user/users.txt");
			//HDFSUtil.uploadFile("D:\\aTemp\\WishList\\school_report.txt", "/user/hbase/source/report/school_report.txt");
			//HDFSMain.uploadFile("D:\\aTemp\\schame\\DB_EDI_Seller_BasicInfo.avsc", "/etljob/schema/avro/DB_EDI_Seller_BasicInfo.avsc");
			//HDFSUtil.uploadFile("D:\\aTemp\\schame\\HBase_User.avsc", "/etljob/reason_test/schema/avro/HBase_User.avsc");
			//HDFSMain.uploadFile("C:\\Users\\rd87\\Desktop\\Job\\etl\\schema\\MG_Base_And_Price.avsc", "/etljob/endeca_test/schema/avro/MG_Base_And_Price.avsc");
			//HDFSMain.downloadFile("/etljob/endeca_test/schema/avro", "C:\\Users\\rd87\\Desktop\\Job\\etl\\schema");
			
			//HDFSUtil.mkdir("/user/hbase/temp/user_test");
			//HDFSUtil.delete("/user/ecappservice/etljob/customer_review/schema/avro/user_test.avsc");
			//HDFSMain.copyCrossHDFS("hdfs://172.16.31.131:8020/etljob/endeca_test/schema/avro/", "hdfs://172.16.59.132:8020/etljob/endeca_test/schema/avro/");
			//HDFSMain.copyCrossHDFS("hdfs://10.16.238.82:8020/etljob/schema/avro/", "hdfs://10.16.238.92:8020/etljob/endeca/schema/avro/");
			//HDFSMain.copyCrossHDFS("hdfs://172.16.31.131:8020/etljob/schema/avro/", "hdfs://172.16.31.131:8020/etljob/endeca_test/schema/avro/");
			//HDFSMain.copyFile("hdfs://ssspark02:8020/etljob/schema/avro/MG_Base_And_Price.avsc", "hdfs://10.16.238.82:8020/etljob/schema/avro/MG_Base_And_Price.avsc");
			//HDFSUtil.delete("/user/ecappservice/etljob/customer_review/temp");
			//HDFSMain.readFile("/user/reason/out/part-r-00000");
			//HDFSMain.appendFile("/user/reason/test/file.txt"," Sam");
			//HDFSMain.readFile("/etljob/schema/avro/DB_DM_EC_Keyword_Profile.avsc");
			
			//HDFSMain.readFile("/etljob/searchpartial/startTime");
			//HDFSMain.readFile("/etljob/searchpartial_test/temp/2015-07-10_01-00-00/itemNumber");
			//HDFSMain.writeFile("/etljob/searchpartial_test/startTime","2015-08-08 12:00:00");
			//HDFSMain.writeFile("/user/reason/test/date.txt","2015-08-06 12:10:00\r\n2015-08-08 12:00:00");
			//initPartialHDFS();
			//initEndecaHDFS();
			

			//HDFSMain.copyCrossHDFS("/user/hive/warehouse/itempartial.db/db_ec_imt_itemusefullinks_mkt", "/user/hive/warehouse/reason.db/db_ec_imt_itemusefullinks_mkt");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	

	
}
