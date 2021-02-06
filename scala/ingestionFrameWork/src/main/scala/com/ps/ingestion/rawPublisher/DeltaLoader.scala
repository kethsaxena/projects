package com.ps.rawPublisher

import com.surescripts.edm.ingestion.rawPublisher.FullLoader
 
class DeltaLoader(props: Map[String,String]) extends FullLoader(props) {

	def printDelta():Unit= {
		println("Hi I am Delta Loader")
	}

	override  def printFull():Unit= {
		println("Hi I am Delta Loader, pretending to be Full")
	}

	// override def printProp():Unit={
	// 	    println("Delta Loader Properties")
 //            for ((k,v) <- props) printf("%s=%s\n", k, v)
 //        }

}