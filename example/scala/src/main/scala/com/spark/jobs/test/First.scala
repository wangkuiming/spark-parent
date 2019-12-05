package com.qiaoda.jobs.test

import com.spark.base.ApplicationBase
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import com.spark.base.ApplicationInterface
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

class first extends ApplicationBase {
  override def run(sc: SparkContext, spark: SparkSession, args: Array[String]): Unit = {

    val arr = Array("1", "2", "1", "2", "1", "2")

    sc.makeRDD(arr).foreach(println);

  }
}
