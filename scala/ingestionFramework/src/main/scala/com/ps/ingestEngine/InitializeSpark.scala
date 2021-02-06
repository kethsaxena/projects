package com.surescripts.edm.ingestEngine

import com.surescripts.hadoop.utils.SparkConfUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object InitializingSpark {

  def InitializeSparkSession(jobName: String): (SparkConf, SparkSession) =  {

    val sparkConf = new SparkConf().setAppName(jobName)
    val envPropertiesFilename = if (new java.io.File("env.properties").exists()) {
      "env.properties"
    } else {
      "../conf/env.properties"
    }
    val props = SparkConfUtil.readProperties(envPropertiesFilename)
    SparkConfUtil.appendSparkConf(sparkConf, props)
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    (sparkConf,sparkSession)
  }

}
