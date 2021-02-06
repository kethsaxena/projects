
package com.surescripts.edm.ingestion.propertiesProducer
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Properties
import java.io.FileInputStream
import scala.collection.JavaConverters._
import scala.collection.{mutable=>m}
import scala.collection.{immutable=>im}


class LoaderDriverProperties(args: Array[String]){
        ///HELPER FUNCTIONS 
         def getPropFile(inputFile : String = "/home/praketa.saxena/sparkScalaProjects/appMod/env/env.properties") : String ={
             var appConfig:String=""
             if (args.length == 0) {
              
                appConfig = inputFile
            } else {
                appConfig = args(0)
            }

            return appConfig
         }
        //COSNTRUCTS  MODIFED PATH DEPENDING ON DISCREPENCIES IN PROPERTIES FILES
        // EX: HANDLES CASES IF PROCJECT HOME = /data01/edm1poc/app/ VS /data01/edm1poc/app 
        def modLoc(projLoc:String,tarLoc:String):String={
            var modLoc ="" 
            if(projLoc.takeRight(1)=="/" && tarLoc.substring(0,1)=="/" ) {
                modLoc=projLoc+tarLoc.substring(1)} 
            else if(projLoc.takeRight(1)=="" && tarLoc.substring(0,1)=="") {
                modLoc=projLoc+"/"+tarLoc}
            else {modLoc=projLoc+tarLoc}
             

            return modLoc
        }
        def getProperties(): Map[String,String] ={
            val properties: Properties = new Properties()
            properties.load(new FileInputStream(getPropFile()))  
            

            var obj = m.Map[String, String]()
            obj = m.Map((properties.asScala.toMap).toSeq: _*)
          
            
            val prop2Change: List[String] = List("EnvDBconnFile", 
                                                "Avro2HiveMapping", 
                                                "AvscFilesLocation",
                                                "InputTablesFile",
                                                "LoadHistoryLocation"
                                            )

            for(item <-prop2Change){
                obj(item)=modLoc(obj("ProjectHome"),obj(item))
            } 

            return obj.toMap

        } 

        def printProp():Unit={
            for ((k,v) <- getProperties()) printf("%s=%s\n", k, v)
        }




        

}

