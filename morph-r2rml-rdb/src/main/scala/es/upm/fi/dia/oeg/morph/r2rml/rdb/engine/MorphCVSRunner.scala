package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.io.Writer
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.MorphProperties

import es.upm.fi.dia.oeg.morph.base.engine.{AbstractQueryResultTranslator, IQueryTranslator, MorphBaseRunner}
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

/**
  * Created by freddy on 14/04/16.
  */
class MorphCVSRunner (
  mappingDocument:R2RMLMappingDocument
  , unfolder:MorphRDBUnfolder
  , dataTranslator:Option[MorphRDBDataTranslator]
  , queryTranslator:Option[IQueryTranslator]
  , resultProcessor:Option[AbstractQueryResultTranslator]
  , outputStream:Writer
) extends MorphRDBRunner(
  mappingDocument
  , unfolder
  , dataTranslator
  , queryTranslator
  , resultProcessor
  , outputStream
) {

}

object MorphCVSRunner {
	val logger = Logger.getLogger("MorphCVSRunner");
	
	def apply(properties:MorphProperties ) : MorphCVSRunner = {
		val runnerFactory = new MorphCVSRunnerFactory();
		val runner = runnerFactory.createRunner(properties);
		runner.asInstanceOf[MorphCVSRunner];
	}
	
	def apply(configurationDirectory:String , configurationFile:String ) : MorphCVSRunner = {
		val configurationProperties = 
				MorphProperties.apply(configurationDirectory, configurationFile);
		val runner = MorphCVSRunner(configurationProperties)
		runner
	}
	
	def main(args:Array[String]) {
		try {
			if(args == null || args.length == 0 || args.length != 2) {
				logger.info("usage MorphCVSRunner propertiesDirectory propertiesFile");
				logger.info("Bye");
				System.exit(-1);
			}
			
			val configurationDirectory = args(0);
			logger.debug("propertiesDirectory = " + configurationDirectory);
			
			val configurationFile = args(1);
			logger.debug("propertiesFile = " + configurationFile);
			
			val runner = MorphCVSRunner(configurationDirectory, configurationFile);
			runner.run();
		} catch {
		  case e:Exception => {
			e.printStackTrace();
			logger.error("Exception occured: " + e.getMessage());
			throw e;		    
		  }

		}
	}
  
}
