package com.spark.base;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

public class SparkStartBase extends SparkBase {

    protected final static String CONF_PATH = "/application.properties";
//	private final static String CLASS_PREFIX = "com.qiaoda.jobs.";
//	private final static String CLASS_SUFFIX = "Application";

    private static void start(String applicationClassName, JavaSparkContext jsc, SparkSession spark, String[] args)
            throws Exception {
        Class<?> clazz = Class.forName(applicationClassName);

        Object instance = clazz.newInstance();

        if (instance instanceof ApplicationInterface) {
            ApplicationInterface application = (ApplicationInterface) instance;
            System.out.println("程序运行开始!");
            application.run(jsc, spark, args);
            application.run(jsc.sc(), spark, args);
            jsc.close();
            spark.stop();
            fs.close();
            System.out.println("恭喜你,程序成功运行完成!");
        } else {
            throw new RuntimeException(applicationClassName + " 必须 implement interface: ApplicationInterface");
        }

    }

    protected static void start(String fullClassName, String appName, Properties properties, String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        // 判断操作系统类型
        String os = System.getProperty("os.name");
        if (os.toLowerCase().startsWith("win")) {
            conf.setMaster("local[10]");
        }
        try {
            fs = FileSystem.get(new Configuration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        conf.set("spark.kryoserializer.buffer", "1024");
        conf.set("spark.kryoserializer.buffer.max", "1024");
//设置应用名
        if (StringUtils.isEmpty(appName)) {
            appName = "spark_" + fullClassName;
        }
        conf.setAppName(appName);

        Enumeration<?> e = properties.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            String value = properties.getProperty(key);
            conf.set(key, value);
        }
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();
        start(fullClassName, jsc, spark, args);
    }
}
