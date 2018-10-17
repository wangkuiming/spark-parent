package com.spark.qiaoda.base;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkUtils {
	/**
	 * 
	 * @param path 路径
	 * @param flag 是否强制删除重写路径
	 * @param rdd 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	protected static  void saveAsTextFile(String path, boolean flag, JavaRDD<?> rdd) throws IOException, URISyntaxException {
		Path p = new Path(path);
		// // 获取文件系统
		FileSystem fileSystem = p.getFileSystem(new Configuration());
		boolean exists = fileSystem.exists(p);
		if (exists) {
			if (flag) {
				fileSystem.delete(p, true);
			} else {
				return;
			}
		}

		rdd.saveAsTextFile(path);
	}



	protected static  void saveAsTextFile(String path, JavaRDD<?> rdd) throws IOException, URISyntaxException {
		// 判断操作系统类型
		String os = System.getProperty("os.name");
		if (os.toLowerCase().startsWith("win")) {
			return;
		}
		saveAsTextFile(path, true, rdd);
	}

	/**
	 * 
	 * @param path 路径
	 * @param flag 是否强制删除重写路径
	 * @param rdd 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	protected static  void saveAsJsonFile(String path, boolean flag, Dataset<Row> rdd) throws IOException, URISyntaxException {
		Path p = new Path(path);
		// // 获取文件系统
		FileSystem fileSystem = p.getFileSystem(new Configuration());
		boolean exists = fileSystem.exists(p);
		if (exists) {
			if (flag) {
				fileSystem.delete(p, true);
			} else {
				return;
			}
		}
		
		rdd.write().json(path);

	}

	protected static  void saveAsParquet(String path, boolean flag, Dataset<Row> rdd) throws IOException, URISyntaxException {
		Path p = new Path(path);
		// // 获取文件系统
		FileSystem fileSystem = p.getFileSystem(new Configuration());
		boolean exists = fileSystem.exists(p);
		if (exists) {
			if (flag) {
				fileSystem.delete(p, true);
			} else {
				return;
			}
		}
		rdd.write().parquet(path);

	}

	protected static  void saveAsParquet(String path, Dataset<Row> rdd) throws IOException, URISyntaxException {
		saveAsParquet(path,true,rdd);

	}



	protected static  void saveAsJsonFile(String path, Dataset<Row> rdd) throws IOException, URISyntaxException {
		saveAsJsonFile(path, true, rdd);
	}

}
