package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.QueryTranslationOptimizerFactory
import java.io.OutputStream
import java.io.Writer

class MorphRDBRunnerFactory extends MorphBaseRunnerFactory{
  
	override def createRunner(mappingDocument:MorphBaseMappingDocument
//    , dataSourceReader:MorphBaseDataSourceReader
    , unfolder:MorphBaseUnfolder
    , dataTranslator :Option[MorphBaseDataTranslator]
//    , materializer : MorphBaseMaterializer
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
	, outputStream:Writer
    ) : MorphRDBRunner = { 
	  new MorphRDBRunner(mappingDocument.asInstanceOf[R2RMLMappingDocument]
//    , dataSourceReader
    , unfolder.asInstanceOf[MorphRDBUnfolder]
    , dataTranslator.asInstanceOf[Option[MorphRDBDataTranslator]]
//    , materializer
    , queryTranslator
    , resultProcessor
    , outputStream
    )
	}
	
	override def readMappingDocumentFile(mappingDocumentFile:String
	    ,props:MorphProperties, connection:Connection ) 
	: MorphBaseMappingDocument = {
		val mappingDocument = R2RMLMappingDocument(mappingDocumentFile, props
		    , connection);
		mappingDocument
	}
	
	override def createUnfolder(md:MorphBaseMappingDocument, dbType:String):MorphRDBUnfolder = {
		val unfolder = new MorphRDBUnfolder(md.asInstanceOf[R2RMLMappingDocument]);
		unfolder.dbType = dbType
		unfolder;	  
	}

	override def createDataTranslator(mappingDocument:MorphBaseMappingDocument
	    , materializer:MorphBaseMaterializer, unfolder:MorphBaseUnfolder
	    , dataSourceReader:MorphBaseDataSourceReader
	    , connection:Connection, properties:MorphProperties)
	:MorphBaseDataTranslator = {
			new MorphRDBDataTranslator(mappingDocument.asInstanceOf[R2RMLMappingDocument]
			, materializer , unfolder.asInstanceOf[MorphRDBUnfolder]
			, dataSourceReader.asInstanceOf[MorphRDBDataSourceReader] , connection, properties);	  
	}
}

object MorphRDBRunnerFactory {
	def createR2RMLRunnerC(configurationDirectory:String , configurationFile:String) 
	: MorphBaseRunner = {
		val properties = MorphProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = MorphRDBRunnerFactory.createR2RMLRunnerC(properties);
		return r2rmlRunner;
	}

	def createR2RMLRunnerC(properties:MorphProperties) 
	: MorphBaseRunner = {
		val runnerFactory = new MorphRDBRunnerFactory();
		val r2rmlRunner = runnerFactory.createRunner(properties);
		val queryTranslator = r2rmlRunner.queryTranslator
		if(queryTranslator.isDefined) {
			val queryTranslationOptimizerC = 
					QueryTranslationOptimizerFactory.createQueryTranslationOptimizerC();
			queryTranslator.get.optimizer = queryTranslationOptimizerC;		  
		}

		r2rmlRunner;
	}

	def createR2RMLRunnerE(configurationDirectory:String, configurationFile:String) : MorphBaseRunner = {
		val properties = 
				MorphProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = MorphRDBRunnerFactory.createR2RMLRunnerE(properties);
		r2rmlRunner;
	}
	
	def  createR2RMLRunnerE(properties:MorphProperties) : MorphBaseRunner = {
		val runnerFactory = new MorphRDBRunnerFactory();
		val r2rmlRunner = runnerFactory.createRunner(properties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.queryTranslator;
		if(queryTranslator.isDefined) {
			val queryTranslationOptimizerE = 
					QueryTranslationOptimizerFactory.createQueryTranslationOptimizerE();
			queryTranslator.get.optimizer = queryTranslationOptimizerE;  
		}

		return r2rmlRunner;
	}

	def createR2RMLRunnerFC(configurationDirectory:String, configurationFile:String) : MorphBaseRunner = {
		val properties = 
				MorphProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = MorphRDBRunnerFactory.createR2RMLRunnerFC(properties);
		r2rmlRunner;
	}

	def createR2RMLRunnerFC(properties:MorphProperties ) : MorphBaseRunner = {
		val runnerFactory = new MorphRDBRunnerFactory();
		val r2rmlRunner = runnerFactory.createRunner(properties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.queryTranslator;
		if(queryTranslator.isDefined) {
			val queryTranslationOptimizerFC = 
					QueryTranslationOptimizerFactory.createQueryTranslationOptimizerFC();
			queryTranslator.get.optimizer = queryTranslationOptimizerFC;		  
		}

		r2rmlRunner;
	}
	
	def createR2RMLRunnerFE(configurationDirectory:String, configurationFile:String) : MorphBaseRunner = {
		val properties = 
				MorphProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = MorphRDBRunnerFactory.createR2RMLRunnerFE(properties);
		r2rmlRunner;
	}

	def createR2RMLRunnerFE(properties:MorphProperties ) : MorphBaseRunner = {
		val runnerFactory = new MorphRDBRunnerFactory();
		val r2rmlRunner = runnerFactory.createRunner(properties);
//		r2rmlRunner.buildQueryTranslator();
		val queryTranslator = r2rmlRunner.queryTranslator;
		if(queryTranslator.isDefined) {
			val queryTranslationOptimizerFE = 
					QueryTranslationOptimizerFactory.createQueryTranslationOptimizerFE();
			queryTranslator.get.optimizer = queryTranslationOptimizerFE;		  
		}

		r2rmlRunner;
	}
}