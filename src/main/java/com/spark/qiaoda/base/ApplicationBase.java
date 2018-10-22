package com.spark.qiaoda.base;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ApplicationBase extends SparkBase implements ApplicationInterface {

	@Override
	public void run(JavaSparkContext jsc, SparkSession spark, String[] args) throws Exception {
	}

	@Override
	public void run(SparkContext sc, SparkSession spark, String[] args) throws Exception {
	}

}
