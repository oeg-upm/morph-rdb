package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractRunner
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLMappingDocument

class R2RMLRunner extends AbstractRunner {
	val logger = Logger.getLogger("R2RMLRunner");
	
	override def createDataTranslator(configurationProperties:ConfigurationProperties ) = {
		this.dataTranslator = new R2RMLDataTranslator(configurationProperties);
		if(this.unfolder != null) {
			this.dataTranslator.setUnfolder(unfolder);
		}
	}

	override def createUnfolder() : AbstractUnfolder  = {
		val unfolder = new R2RMLUnfolder();
		unfolder.setDbType(this.configurationProperties.databaseType);
		return unfolder;
	}

	override def readMappingDocumentFile(mappingDocumentFile:String ) = {
		this.mappingDocument = new R2RMLMappingDocument(mappingDocumentFile, configurationProperties);
	}

}

object R2RMLRunner {
	val logger = Logger.getLogger("R2RMLRunner");
	
	def main(args:Array[String]) {
		try {
			if(args == null || args.length == 0 || args.length != 2) {
				logger.info("usage R2RMLRunner propertiesDirectory propertiesFile");
				System.exit(-1);
			}
			
			val configurationDirectory = args(0);
			logger.debug("propertiesDirectory = " + configurationDirectory);
			
			val configurationFile = args(1);
			logger.debug("propertiesFile = " + configurationFile);
			
			val runner = new R2RMLRunner();
			runner.loadConfigurationfile(configurationDirectory, configurationFile);
			runner.run();
		} catch {
		  case e:Exception => {
			//e.printStackTrace();
			logger.error("Exception occured: " + e.getMessage());
			throw e;		    
		  }

		}
	}
  
}