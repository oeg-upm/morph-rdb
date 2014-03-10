package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.materializer.MaterializerFactory
import es.upm.fi.dia.oeg.morph.base.MorphProperties

abstract class MorphBaseRunnerFactory {
	val logger = Logger.getLogger(this.getClass());
	
	def createRunner(configurationDirectory:String , configurationFile:String ) 
	: MorphBaseRunner = {
		val configurationProperties = MorphProperties.apply(
		    configurationDirectory, configurationFile);
		this.createRunner(configurationProperties);
	}
	
	def createRunner(properties:MorphProperties) : MorphBaseRunner = {
		//BUILDING CONNECTION
		val connection = this.createConnection(properties);

		//BUILDING CONNECTION AND DATA SOURCE READER
		val dataSourceReaderClassName = properties.queryEvaluatorClassName;
		val dataSourceReader = MorphBaseDataSourceReader(dataSourceReaderClassName, connection
		    , properties.databaseTimeout);

		//BUILDING MAPPING DOCUMENT
		val mappingDocumentFile = properties.mappingDocumentFilePath;
		val mappingDocument = this.readMappingDocumentFile(mappingDocumentFile, properties, connection);

		//BUILDING UNFOLDER
		val unfolder = this.createUnfolder(mappingDocument, properties.databaseType);
		
		//BUILDING MATERIALIZER
		val materializer = this.buildMaterializer(properties, mappingDocument);
		
		//BUILDING DATA TRANSLATOR
		val dataTranslator = this.createDataTranslator(mappingDocument, materializer, unfolder
    , dataSourceReader, connection, properties);
		
		//BUILDING QUERY TRANSLATOR
		logger.info("Building query translator...");
		val queryTranslatorFactoryClassName = 
		  properties.queryTranslatorFactoryClassName;
		val queryTranslator = try {
			  val qtAux = this.buildQueryTranslator(queryTranslatorFactoryClassName
					  , mappingDocument, connection, properties);
			  Some(qtAux);
		  } catch {
		    case e:Exception => {
		      logger.warn("Error building query translator!");
		    }
		    None
		  }

		//BUILDING QUERY RESULT WRITER
		val queryResultWriter = if(queryTranslator.isDefined) {
			val queryResultWriterFactoryClassName = 
			  properties.queryResultWriterFactoryClassName;
			val outputFileName = properties.outputFilePath;
			val qrwAux = this.buildQueryResultWriter(queryResultWriterFactoryClassName
			    , queryTranslator.get, outputFileName);
			Some(qrwAux)
		} else { None }

		//BUILDING RESULT PROCESSOR
		val resultProcessor = if(queryResultWriter.isDefined) {
			val resultProcessorAux = this.buildQueryResultTranslator(dataSourceReader
					, queryResultWriter.get);
			Some(resultProcessorAux)
		} else { None }
		
		val runner = this.makeRunner(mappingDocument, dataSourceReader, unfolder
		    , dataTranslator, materializer, queryTranslator,  resultProcessor)
		runner.ontologyFilePath = properties.ontologyFilePath;
		runner;
	}
	
	def makeRunner(mappingDocument:MorphBaseMappingDocument
    , dataSourceReader:MorphBaseDataSourceReader
    , unfolder:MorphBaseUnfolder
    , dataTranslator :MorphBaseDataTranslator
    , materializer : MorphBaseMaterializer
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
    ) : MorphBaseRunner;
	
	def readMappingDocumentFile(mappingDocumentFile:String
	    ,props:MorphProperties, connection:Connection)
	:MorphBaseMappingDocument
	    
	def createUnfolder(md:MorphBaseMappingDocument, dbType:String):MorphBaseUnfolder;
	
	def createDataTranslator(md:MorphBaseMappingDocument, materializer:MorphBaseMaterializer
	    , unfolder:MorphBaseUnfolder, dataSourceReader:MorphBaseDataSourceReader
	    , connection:Connection, properties:MorphProperties):MorphBaseDataTranslator;
	
	def createConnection(configurationProperties:MorphProperties) : Connection = {
		val connection = if(configurationProperties.noOfDatabase > 0) {
			val databaseUser = configurationProperties.databaseUser;
			val databaseName = configurationProperties.databaseName;
			val databasePassword = configurationProperties.databasePassword;
			val databaseDriver = configurationProperties.databaseDriver;
			val databaseURL = configurationProperties.databaseURL;
			DBUtility.getLocalConnection(databaseUser, databaseName, databasePassword, 
					databaseDriver, databaseURL, "Runner");
		} else {
		  null
		}

		connection;
	}	
	

	def buildQueryTranslator(queryTranslatorFactoryClassName:String
	    , md:MorphBaseMappingDocument, connection:Connection
	    , configurationProperties:MorphProperties) : IQueryTranslator = {
		val className = if(queryTranslatorFactoryClassName == null || queryTranslatorFactoryClassName.equals("")) {
			Constants.QUERY_TRANSLATOR_FACTORY_CLASSNAME_DEFAULT;
		} else {
			queryTranslatorFactoryClassName; 
		}

		val queryTranslatorFactory = Class.forName(className).newInstance().asInstanceOf[IQueryTranslatorFactory]; 
		val queryTranslator = queryTranslatorFactory.createQueryTranslator(
		    md, connection);

		//query translation optimizer
		val queryTranslationOptimizer = this.buildQueryTranslationOptimizer();
		val eliminateSelfJoin = configurationProperties.selfJoinElimination;
		queryTranslationOptimizer.selfJoinElimination = eliminateSelfJoin;
		val eliminateSubQuery = configurationProperties.subQueryElimination;
		queryTranslationOptimizer.subQueryElimination = eliminateSubQuery;
		val transJoinEliminateSubQuery = configurationProperties.transJoinSubQueryElimination;
		queryTranslationOptimizer.transJoinSubQueryElimination = transJoinEliminateSubQuery;
		val transSTGEliminateSubQuery = configurationProperties.transSTGSubQueryElimination;
		queryTranslationOptimizer.transSTGSubQueryElimination = transSTGEliminateSubQuery;
		val subQueryAsView = configurationProperties.subQueryAsView;
		queryTranslationOptimizer.subQueryAsView = subQueryAsView;
		queryTranslator.optimizer = queryTranslationOptimizer;
		logger.debug("query translator = " + queryTranslator);
		
		//sparql query
		val queryFilePath = configurationProperties.queryFilePath;
		queryTranslator.setSPARQLQueryByFile(queryFilePath);
		
		queryTranslator
	}
	
	def buildQueryResultWriter(queryResultWriterFactoryClassName:String
	    , queryTranslator:IQueryTranslator, pOutputFileName:String)
	: MorphBaseQueryResultWriter = {
		val className = if(queryResultWriterFactoryClassName == null 
				|| queryResultWriterFactoryClassName.equals("")) {
			Constants.QUERY_RESULT_WRITER_FACTORY_CLASSNAME_DEFAULT;
		} else {
			queryResultWriterFactoryClassName; 
		}
		
		val queryResultWriterFactory = Class.forName(className).newInstance().asInstanceOf[QueryResultWriterFactory];
		val queryResultWriter = queryResultWriterFactory.createQueryResultWriter(queryTranslator);

		//set output file
		val outputFileName = if(pOutputFileName == null) {
			Constants.QUERY_RESULT_XMLWRITER_OUTPUT_DEFAULT;
		} else { pOutputFileName }
		
		queryResultWriter.setOutput(outputFileName);

		logger.debug("query result writer = " + queryResultWriter);
		queryResultWriter
	}
	
	def buildQueryTranslationOptimizer() : QueryTranslationOptimizer = {
		new QueryTranslationOptimizer();
	}
	
	def buildQueryResultTranslator(dataSourceReader:MorphBaseDataSourceReader
	    , queryResultWriter:MorphBaseQueryResultWriter) 
	: AbstractQueryResultTranslator  =  {
		val className = Constants.QUERY_RESULT_TRANSLATOR_CLASSNAME_DEFAULT;

		val queryResultTranslatorFactory = Class.forName(className).newInstance().asInstanceOf[AbstractQueryResultTranslatorFactory]; 
		val queryResultTranslator = queryResultTranslatorFactory.createQueryResultTranslator(
				dataSourceReader, queryResultWriter);
		queryResultTranslator;
		
	}
	
	def buildMaterializer(configurationProperties:MorphProperties
	    , mappingDocument:MorphBaseMappingDocument) 
	: MorphBaseMaterializer = {
		val outputFileName = configurationProperties.outputFilePath;
		val rdfLanguage = configurationProperties.rdfLanguage;
		val jenaMode = configurationProperties.jenaMode;
		val materializer = MaterializerFactory.create(rdfLanguage, outputFileName, jenaMode);
		val mappingDocumentPrefixMap = mappingDocument.mappingDocumentPrefixMap; 
		if(mappingDocumentPrefixMap != null) {
			materializer.setModelPrefixMap(mappingDocumentPrefixMap);
		}
		materializer
	}	
}

