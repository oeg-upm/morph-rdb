package es.upm.fi.dia.oeg.morph.base.engine

import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.Model
import java.io.FileOutputStream

class ModelWriter {
	val logger = Logger.getLogger(this.getClass().getName());
	

}

object ModelWriter {
  	val logger = Logger.getLogger(this.getClass().getName());

	def writeModelStream(model:Model , outputFilename:String , rdfLanguage:String ) {
		try {
			if(model != null) {
				logger.debug("Size of model = " + model.size());
				logger.info("Writing model to " + outputFilename + " ......");
				val startWritingModel = System.currentTimeMillis();
				val resultOutputStream = new FileOutputStream(outputFilename);
				model.write(resultOutputStream, rdfLanguage);
				val endWritingModel = System.currentTimeMillis();
				val durationWritingModel = (endWritingModel-startWritingModel) / 1000;
				logger.info("Writing model time was "+(durationWritingModel)+" s.");				
			}
		} catch {
		  case e:Exception => {
			logger.error("Error writing model because " + e.getMessage());
			throw e;		    
		  }
		}
	}

  
}