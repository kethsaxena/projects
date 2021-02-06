//  OWNER: EDM TEAM 1 
//  CHANGE DATE: 2/5/2021 
//  AUTHOR: PRAKETA SAXENA   
//  MAIN: IngestionExecution
	// SUBCLASS: LoaderDriverProperties 	
	// SUBCLASS: RawDriver 
	// SUBLCASS:FullLoader
		// CHILD SUBCLASS: DeltaLoader
	// SUBCLASS: Utilities 
	

package com.surescripts.edm.ingestion

//STANDARD LIBRARY 
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.log4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import java.util.Date
import java.util.Properties
import java.util._
import java.text.SimpleDateFormat
import java.io._
import scala.util.parsing.json.JSON
import java.nio.file.{Paths, Files}
import scala.annotation.varargs
import upickle.default._
import os._
import java.sql.Timestamp

//CUSTOM CLASSES
import com.surescripts.edm.ingestion.propertiesProducer.LoaderDriverProperties
import com.surescripts.edm.ingestion.rawPublisher.RawDriver

object IngestionExecution {

    def main(args: Array[String]) {

        //STEP 1 LOAD PROPERTIES
        val props = new LoaderDriverProperties(args)
    
        //STEP 2 PUBLISH  DATA IN DATA LAKE
        val obj = new RawDriver(props.getProperties())


      }

             
}
