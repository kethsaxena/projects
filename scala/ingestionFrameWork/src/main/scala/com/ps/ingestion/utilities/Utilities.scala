
package com.ps.utilites

import org.apache.log4j.Logger
import org.slf4j.LoggerFactory


object Logger {

 	val logger = LoggerFactory.getLogger(this.getClass.getName)
  	val appName = "LoadDataUsingCSVschema"
    logger.info(appName)
    logger.info(s"LoadDataUsingCSVschema: event=startApplication")
    
  	def info(message: String): Unit = println(s"INFO: $message")

  	def debug(message: String): Unit = println(s"DEBUG: $message")

  	def trace(message: String): Unit = println(s"TRACE: $message")



}