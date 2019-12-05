package com.spark.base;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public interface ApplicationInterface {
	void run(JavaSparkContext jsc, SparkSession spark, String[] args) throws Exception;
	void run(SparkContext sc, SparkSession spark, String[] args) throws Exception;
}
