package com.ps.rawPublisher




class RawDriver(props: Map[String,String]){

	val loadType="FULL"

	if(loadType=="FULL"){
		import com.surescripts.edm.ingestion.rawPublisher.FullLoader
		val publish = new FullLoader(props)
   		//DRIVER FUNCTION OF FULL LOADER
   		publish.rawPublish()
	}
	else{
		import com.surescripts.edm.ingestion.rawPublisher.DeltaLoader
	    val publish2 = new DeltaLoader(props)    
	}

	
}