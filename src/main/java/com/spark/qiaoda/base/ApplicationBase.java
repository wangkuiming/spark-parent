package com.spark.qiaoda.base;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public abstract class ApplicationBase extends SparkBase implements ApplicationInterface {

	@Override
	public abstract void run(JavaSparkContext jsc, SparkSession spark, String[] args) throws Exception;

	@Override
	public final void run(SparkContext sc, SparkSession spark, String[] args) throws Exception {
	}

}
