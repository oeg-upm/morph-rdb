package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.io.Writer
//import java.util.Properties;
import es.upm.fi.dia.oeg.morph.base.engine.{AbstractQueryResultTranslator, IQueryTranslator, MorphBaseRunner}
import es.upm.fi.dia.oeg.morph.base.MorphBenchmarking
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import org.slf4j.LoggerFactory

/**
  * Created by freddy on 14/04/16.
  */
class MorphCSVRunner (
  mappingDocument:R2RMLMappingDocument
  , unfolder:MorphRDBUnfolder
  , dataTranslator:Option[MorphRDBDataTranslator]
  , queryTranslator:Option[IQueryTranslator]
  , resultProcessor:Option[AbstractQueryResultTranslator]
  , outputStream:Writer
	, benchmark: MorphBenchmarking
) extends MorphRDBRunner(
  mappingDocument
  , unfolder
  , dataTranslator
  , queryTranslator
  , resultProcessor
  , outputStream
	, benchmark
) {

}

object MorphCSVRunner {
  val logger = LoggerFactory.getLogger(this.getClass());
	
	def apply(properties:MorphCSVProperties) : MorphCSVRunner = {
		val runnerFactory = new MorphCSVRunnerFactory();
		val runner = runnerFactory.createRunner(properties);
		runner.asInstanceOf[MorphCSVRunner];
	}
	
	def apply(configurationDirectory:String , configurationFile:String ) : MorphCSVRunner = {
		val configurationProperties = 
				MorphCSVProperties.apply(configurationDirectory, configurationFile);
		val runner = MorphCSVRunner(configurationProperties)
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
			
			val runner = MorphCSVRunner(configurationDirectory, configurationFile);
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
