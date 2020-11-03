package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.{MorphBenchmarking, MorphProperties}
import java.sql.Connection

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.QueryTranslationOptimizerFactory
import java.io.OutputStream
import java.io.Writer
//import java.util.Properties

class MorphRDBRunnerFactory extends MorphBaseRunnerFactory{

	override def createRunner(mappingDocument:MorphBaseMappingDocument
			, unfolder:MorphBaseUnfolder
			, dataTranslator:Option[MorphBaseDataTranslator]
      , queryTranslator:Option[IQueryTranslator]
      , resultProcessor:Option[AbstractQueryResultTranslator]
      , outputStream:Writer
			, benchmark: MorphBenchmarking
			) : MorphBaseRunner = { 
					val morphRDBRunner = new MorphRDBRunner(mappingDocument.asInstanceOf[R2RMLMappingDocument]
							, unfolder.asInstanceOf[MorphRDBUnfolder]
              , dataTranslator.asInstanceOf[Option[MorphRDBDataTranslator]]
              , queryTranslator
              , resultProcessor
              , outputStream
					, benchmark)

							morphRDBRunner;
	}

  override def createRunner(configurationDirectory:String , configurationFile:String) 
	: MorphBaseRunner = {
    
		val configurationProperties = MorphRDBProperties.apply(configurationDirectory, configurationFile);
		this.createRunner(configurationProperties);
	}



	override def readMappingDocumentFile(mappingDocumentFile:String
			,props:MorphProperties, connection:Connection ) 
	: MorphBaseMappingDocument = {
			val mappingDocument = R2RMLMappingDocument(
					mappingDocumentFile, props, connection);
			mappingDocument
	}

	override def createUnfolder(md:MorphBaseMappingDocument, props:MorphProperties):MorphRDBUnfolder = {
	  val morphRDBProperties = props.asInstanceOf[MorphRDBProperties];
	  
    val unfolder = new MorphRDBUnfolder(md.asInstanceOf[R2RMLMappingDocument], morphRDBProperties);
    //unfolder.dbType = props.databaseType;
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
	
//	def createConnection(morphProperties:MorphProperties) : Connection = {
//		val connection = if(morphProperties.noOfDatabase > 0) {
//			val databaseUser = morphProperties.databaseUser;
//			val databaseName = morphProperties.databaseName;
//			val databasePassword = morphProperties.databasePassword;
//			val databaseDriver = morphProperties.databaseDriver;
//			val databaseURL = morphProperties.databaseURL;
//			DBUtility.getLocalConnection(databaseUser, databaseName, databasePassword, 
//					databaseDriver, databaseURL, "Runner");
//		} else {
//		  null
//		}
//
//		connection;
//	}	
	
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