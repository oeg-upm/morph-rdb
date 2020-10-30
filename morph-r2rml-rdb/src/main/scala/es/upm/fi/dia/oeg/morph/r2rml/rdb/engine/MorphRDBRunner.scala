package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

//import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.base.MorphBenchmarking
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import java.io.OutputStream
import java.io.Writer

import org.slf4j.LoggerFactory

//import java.util.Properties


class MorphRDBRunner(mappingDocument:R2RMLMappingDocument
										 //    , dataSourceReader:MorphBaseDataSourceReader
										 , unfolder:MorphRDBUnfolder
										 , dataTranslator:Option[MorphRDBDataTranslator]
										 //    , materializer:MorphBaseMaterializer
										 , queryTranslator:Option[IQueryTranslator]
										 , resultProcessor:Option[AbstractQueryResultTranslator]
										 , outputStream:Writer
										 , benchmark: MorphBenchmarking
										) extends MorphBaseRunner(mappingDocument
	//    , dataSourceReader
	, unfolder
	, dataTranslator
	//    , materializer
	, queryTranslator
	, resultProcessor
	, outputStream
	, benchmark
) {

	//override val logger = Logger.getLogger(this.getClass());


}

object MorphRDBRunner {
	val logger = LoggerFactory.getLogger(this.getClass());

	def apply(properties:MorphRDBProperties ) : MorphRDBRunner = {
		//logger.info("running morph-rdb 3.12.5 ...");

		val runnerFactory = new MorphRDBRunnerFactory();
		//val runner = new R2RMLRunner();
		//		runner.loadConfigurationProperties(properties);
		val runner = runnerFactory.createRunner(properties);
		runner.asInstanceOf[MorphRDBRunner];
	}

	def apply(configurationDirectory:String , configurationFile:String ) : MorphRDBRunner = {
		val configurationProperties = MorphRDBProperties.apply(configurationDirectory, configurationFile);
		val runner = MorphRDBRunner(configurationProperties)
		runner
	}

	def main(args:Array[String]) {

		try {
			if(args == null || args.length == 0 || args.length != 2) {
				logger.info("usage R2RMLRunner propertiesDirectory propertiesFile");
				logger.info("Bye");
				System.exit(-1);
			}

			val configurationDirectory = args(0);
			logger.debug("propertiesDirectory = " + configurationDirectory);

			val configurationFile = args(1);
			logger.debug("propertiesFile = " + configurationFile);

			val runner = MorphRDBRunner(configurationDirectory, configurationFile);
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