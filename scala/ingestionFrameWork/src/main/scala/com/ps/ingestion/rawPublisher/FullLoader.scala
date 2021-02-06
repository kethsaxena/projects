package com.ps.rawPublisher

//SCALA LIBRARIES 
import scala.collection.{mutable=>m}
import scala.collection.JavaConverters._
import scala.io.Source
import os._
import upickle.default._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
//import sparkSession.implicits._
 
//JAVA LIBRARIES 
import java.util.Properties
import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat
import java.io.FileInputStream



class FullLoader(props: Map[String,String]){

    
    // DO NOTHING BECAUSE SCALA DOES NOT HAVE PASS, USED WHEN U WANT TO SKIP PROPERTIES 
    def pass():Unit={}

    def printFull():Unit= {
		println("Full Loader Initiated")
	}

	def printClassProp():Unit={
		    println("Full Loader Properties")
            for ((k,v) <- props) printf("%s=%s\n", k, v)
        }

    def printProp(obj:Properties):Unit={
		    println("Full Loader Properties")
		     val mapProp=m.Map((obj.asScala.toMap).toSeq: _*)
            for ((k,v) <- mapProp) printf("%s=%s\n", k, v)
        }



	def getCurrentdateTimeStamp(): String ={
        val today:java.util.Date = Calendar.getInstance.getTime
        val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val now:String = timeFormat.format(today)
        return now
    }

    def getTodaysPartition: String ={
        val today:java.util.Date = Calendar.getInstance.getTime
        val timeFormat = new SimpleDateFormat("yyyyMMdd")
        val dt:String = "dt=" + timeFormat.format(today)
        dt
    }


    def getTableList(inputFileLoc:String):List[List[String]]={

    	val inputTables = Source.fromFile(inputFileLoc)
    	var tableList = new m.ListBuffer[List[String]]()

      for (line <- inputTables.getLines()) {
            if ((!line.trim.matches("") && !line.trim.matches("#.*"))) {
             tableList.append(List(line.split("\\.")(0),line.split("\\.")(1)))     
            } 
           
        }
       return tableList.toList
    }


     //READ JSON META DATA AS UPICKLE OBJECT 

     def readMetadata(filePath:String):  ujson.Value ={

     	val jsonString = os.read(os.Path(filePath))
     	val obj = ujson.read(jsonString)

     	return obj

      }

      //RETURN CONNECTION PROPERTIES FOR SCHEMA EX PASS IN "SMS" --> GET CONNECTION PROPERTIES ; "FIN_MDM" --> GET CONNECTION PROPERTIES ; 

      def getConnectionProperties(schemaName:String=""):Map[String, String]={
      	var obj = Map[String, String]()
      	var propList = new m.ListBuffer[String]()
      	val propKeys = List("username","password","hostname","port","servicename")
      	val searchStr  = "spark.surescripts."+schemaName+".connection"
      	val propArray = props(searchStr).split("/")

      	for(item <- propArray.indices) {
      		if(item==1){
      			val arr = propArray(item).split("@")
      			for(item <- arr.indices){				
      				if(item!=1){
      				propList.append(arr(item))}
      				else{
      					val arr2 = arr(item).split(":")
      					for(item <- arr2.indices){
      					propList.append(arr2(item))}
      				}	
      			} 
      		}
      		else{
      			propList.append(propArray(item))
      		}
      	}

      	val connMap = (propKeys zip propList).toMap
      	return connMap
      }

      def getConnectionString(connMap:Map[String, String]):String={
		return "jdbc:oracle:thin:@//" + connMap("hostname") + ":" + connMap("port") + "/" + connMap("servicename")
      }


    //GET TARGET SCHEMA   

	  def getTargetSchema(metadata:ujson.Value):List[List[String]]={
	  	//println(metadata)
	  	 var customSchemaDef :String  = "";
         var customColumnDef :String  = "";
	       for(item <- metadata.arr) {
	           customSchemaDef+=item("name").str + " " + getParquetType(item("type")(1).str) + ", "
	           customColumnDef+=item("name").str+","
	       }

	        customSchemaDef = customSchemaDef.dropRight(2)
            customColumnDef = customColumnDef.dropRight(1)
	       return List(List(customSchemaDef),List(customColumnDef))
	     }


      // RETURN PARQUET TYPE SCHEMA FROM AVRO EQUIVALENT   

	  def getParquetType(avroType: String) : String = {
       
        val avro2Hive: Properties = new Properties()
        avro2Hive.load(new FileInputStream(props("Avro2HiveMapping"))) 
        var avro2HiveMap = Map[String, String]()
        avro2HiveMap = Map((avro2Hive.asScala.toMap).toSeq: _*)
        
        return avro2HiveMap(avroType)
    }

    // RETURN SPARK QUERY STRING  


     def getQuery(columnDef:String,tableName:String):String={
     	 val query = "(SELECT " + columnDef + " FROM " + tableName + ") aliasName"
         return query
     }


     def setsparkSessionProp(schema:String):Properties={

     	val pot= getConnectionProperties("fin_mdm")
    

     	val sparkSessProp = new Properties()
        sparkSessProp.put("user", pot("username"))
        sparkSessProp.put("password", pot("password"))
        sparkSessProp.put("driver", "oracle.jdbc.driver.OracleDriver")
        sparkSessProp.put("numPartitions", props("NumberOfConnections"))
        sparkSessProp.put("customSchema", schema)

        return sparkSessProp
        
      }



     def getSparkDF(connString:String,query:String,connectionProperties:Properties):DataFrame={

     	val sparkMaster="local"

    	val sparkSession = SparkSession.builder
                 .master(sparkMaster)
                 .config("hive.exec.dynamic.partition", "true")
                 .config("hive.exec.dynamic.partition.mode", "nonstrict")
                 .config("spark.sql.parquet.writeLegacyFormat", "true")
                 .config("spark.sql.parquet.binaryAsString","true")
                 .enableHiveSupport()
                 .appName("Oracle Data Ingestion")
                 .getOrCreate()

         val df = sparkSession.read.jdbc(connString, query, connectionProperties)
         df.printSchema()
         df.show()

        return df


     }


     def writeHDFS(df:DataFrame,hadoopStorageFormat:String,targetSaveMode:String):Unit={

      val table = getTableList(props("InputTablesFile").trim())
     	val meta = readMetadata(props("AvscFilesLocation")+"/"+table(0)(0)+"/"+table(0)(1)+".avsc")
     	val hdfsPath=props("AppRootHDFS")+"/" +meta("table")("db_name").str.replace("\"", "")+"/tables/"+meta("table")("db_name").str.replace("\"", "")+"/"+getTodaysPartition

    
     	df.write.format(hadoopStorageFormat)
                  .option("path", hdfsPath)
                  .mode(targetSaveMode)
                  .save()
     }

     //CLASS DRIVER FUNCTION 


     def rawPublish():Unit={

     	      //--- STEP 1 READ INPUT LIST----  
		    println("RAW PUBLISHER START")
          	val tableList= getTableList(props("InputTablesFile").trim())

          	//--- STEP 2 READ META DATA ----
          	val obj = readMetadata(props("AvscFilesLocation")+"/"+tableList(0)(0)+"/"+tableList(0)(1)+".avsc")
          	val schema = getTargetSchema(obj("fields"))
          	val table = obj("extract")("source_db").str.replace("\"", "")+"."+tableList(0)(1)
          	println("Table:"+table)
            println(obj("extract")("source_db").str.replace("\"", ""))
          	val connStr = getConnectionString(getConnectionProperties("fin_mdm"))
          	val query = getQuery(schema(1)(0),"account_map")
            
            //--- STEP 3 SET SPARK SESSION PROPERTIES ----
          	val sparkProperties= setsparkSessionProp(schema(0)(0))
          	
            //--- STEP 4 READ SPARK DATA ----
          	val data = getSparkDF(connStr,query,sparkProperties)
          	
            //--- STEP 5 WRITE HDFS DATA LAKE ----
			      writeHDFS(data,"parquet","overwrite")
  
        }





}
