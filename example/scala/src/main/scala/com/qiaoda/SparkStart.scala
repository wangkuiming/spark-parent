package com.qiaoda

import com.spark.qiaoda.base.SparkStartBase
import java.io.FileInputStream
import java.io.File
import java.util.Properties

object SparkStart extends SparkStartBase {

  def main(args: Array[String]): Unit = {

    var classPackage = "mllib";
    var className = "Application";

    var configurationPath = classPackage.concat(SparkStartBase.CONF_PATH)

    var properties: Properties = null;
    var file = new File(configurationPath);
    if (file.exists()) {
      var inputStream = new FileInputStream(file);
      properties = new Properties();
      properties.load(inputStream);
    } else {
      properties = new Properties();
    }

    SparkStartBase.start(classPackage, className, properties, args);

  }
}

