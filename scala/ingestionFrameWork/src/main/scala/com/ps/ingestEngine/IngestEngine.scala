package com.ps.ingestEngine

import org.joda.time.DateTime
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object IngestEngine {

  private val logger: Logger = Logger.getLogger(this.getClass)

  val partitionDateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  def main(args: Array[String]) {

    val (sparkConf, sparkSession) = InitializingSpark.InitializeSparkSession("IngestEngine")

    val date = DateTime.now()

    logger.info("COMS:          " + getConnectionString(sparkConf,"coms"))
    logger.info("Schema path:   " + getSchemaPath(sparkConf,"drugdb_r"))
    logger.info("TablePath:     " + getTablePath(sparkConf, "sdmp_r", "fdbretcndc0"))
    logger.info("PartitionPath: " + getPartitionPath(sparkConf, "sdmp_r", "fdbretcndc0", date))
  }



  def getConnectionString(sparkConf: SparkConf, databasePrefix: String): String = {
    sparkConf.get(s"spark.surescripts.${databasePrefix}.connection")
  }

  def getDatabasePath(sparkConf: SparkConf, databasePrefix: String): String = {
    val suffix = sparkConf.get(s"spark.surescripts.db_suffix")
    return s"/data/${databasePrefix}${suffix}"
  }

  def getSchemaPath(sparkConf: SparkConf, databasePrefix: String): String = {
    return s"${getDatabasePath(sparkConf,databasePrefix)}/schema"
  }

  def getTablePath(sparkConf: SparkConf, databasePrefix: String, table: String): String = {
    return s"${getDatabasePath(sparkConf,databasePrefix)}/tables/${table}"
  }

  def getPartitionPath(sparkConf: SparkConf, databasePrefix: String, table: String, date: DateTime): String = {
    return s"${getTablePath(sparkConf,databasePrefix, table)}/dt=${date.toString(partitionDateFormat)}"
  }
}