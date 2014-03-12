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

class R2RMLRDBRunnerFactory extends MorphBaseRunnerFactory{
  
	override def createRunner(mappingDocument:MorphBaseMappingDocument
//    , dataSourceReader:MorphBaseDataSourceReader
    , unfolder:MorphBaseUnfolder
    , dataTranslator :Option[MorphBaseDataTranslator]
//    , materializer : MorphBaseMaterializer
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
	, outputStream:Writer
    ) : R2RMLRunner = { 
	  new R2RMLRunner(mappingDocument.asInstanceOf[R2RMLMappingDocument]
//    , dataSourceReader
    , unfolder.asInstanceOf[R2RMLUnfolder]
    , dataTranslator.asInstanceOf[Option[R2RMLDataTranslator]]
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
	
	override def createUnfolder(md:MorphBaseMappingDocument, dbType:String):R2RMLUnfolder = {
		val unfolder = new R2RMLUnfolder(md.asInstanceOf[R2RMLMappingDocument]);
		unfolder.dbType = dbType
		unfolder;	  
	}

	override def createDataTranslator(mappingDocument:MorphBaseMappingDocument
	    , materializer:MorphBaseMaterializer, unfolder:MorphBaseUnfolder
	    , dataSourceReader:MorphBaseDataSourceReader
	    , connection:Connection, properties:MorphProperties)
	:MorphBaseDataTranslator = {
			new R2RMLDataTranslator(mappingDocument.asInstanceOf[R2RMLMappingDocument]
			, materializer , unfolder.asInstanceOf[R2RMLUnfolder]
			, dataSourceReader.asInstanceOf[RDBReader] , connection, properties);	  
	}
}

object R2RMLRDBRunnerFactory {
	def createR2RMLRunnerC(configurationDirectory:String , configurationFile:String) 
	: MorphBaseRunner = {
		val properties = MorphProperties.apply(configurationDirectory, configurationFile);
		val r2rmlRunner = R2RMLRDBRunnerFactory.createR2RMLRunnerC(properties);
		return r2rmlRunner;
	}

	def createR2RMLRunnerC(properties:MorphProperties) 
	: MorphBaseRunner = {
		val runnerFactory = new R2RMLRDBRunnerFactory();
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
		val r2rmlRunner = R2RMLRDBRunnerFactory.createR2RMLRunnerE(properties);
		r2rmlRunner;
	}
	
	def  createR2RMLRunnerE(properties:MorphProperties) : MorphBaseRunner = {
		val runnerFactory = new R2RMLRDBRunnerFactory();
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
		val r2rmlRunner = R2RMLRDBRunnerFactory.createR2RMLRunnerFC(properties);
		r2rmlRunner;
	}

	def createR2RMLRunnerFC(properties:MorphProperties ) : MorphBaseRunner = {
		val runnerFactory = new R2RMLRDBRunnerFactory();
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
		val r2rmlRunner = R2RMLRDBRunnerFactory.createR2RMLRunnerFE(properties);
		r2rmlRunner;
	}

	def createR2RMLRunnerFE(properties:MorphProperties ) : MorphBaseRunner = {
		val runnerFactory = new R2RMLRDBRunnerFactory();
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