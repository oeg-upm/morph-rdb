package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.materializer.MaterializerFactory
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.io.Writer
import java.io.FileWriter
import java.io.StringWriter
import com.hp.hpl.jena.query.QueryFactory
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.util.Properties
import es.upm.fi.dia.oeg.morph.r2rml.rdb.mappingsgenerator.main.R2RMLMapper

abstract class MorphBaseRunnerFactory {
	val logger = Logger.getLogger(this.getClass());
	
	def createRunner(configurationDirectory:String , configurationFile:String) 
	: MorphBaseRunner = {
		val configurationProperties = MorphProperties.apply(
		    configurationDirectory, configurationFile);
		this.createRunner(configurationProperties);
	}
	
	def createRunner(properties:Properties):MorphBaseRunner = {
		val morphProperties = properties.asInstanceOf[MorphProperties];
		
		//BUILDING CONNECTION
		val connection = this.createConnection(morphProperties);

		//BUILDING CONNECTION AND DATA SOURCE READER
		val dataSourceReaderClassName = morphProperties.queryEvaluatorClassName;
		val dataSourceReader = MorphBaseDataSourceReader(dataSourceReaderClassName
		    , connection, morphProperties.databaseTimeout);

		//BUILDING MAPPING DOCUMENT
		var automaticMappingsGeneration = false;
		val mappingDocumentFile = try {
			if(morphProperties.mappingDocumentFilePath == null) {
				val mappingsGenerator = new R2RMLMapper();
				mappingsGenerator.run(properties);
				automaticMappingsGeneration = true;
			    mappingsGenerator.getGeneratedMappingsFile();
			} 
			else { morphProperties.mappingDocumentFilePath; }
		}
		catch { case e:Exception => { morphProperties.mappingDocumentFilePath; } }
		
		val mappingDocument = this.readMappingDocumentFile(mappingDocumentFile
		    , morphProperties, connection);

		//BUILDING UNFOLDER
		val unfolder = this.createUnfolder(mappingDocument, morphProperties);
		
		val outputStream:Writer = if(morphProperties.outputFilePath.isDefined) {
			val outputFileName = morphProperties.outputFilePath.get;
		  //new FileWriter(outputFileName)
			//new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFileName), "UTF-8"));
			new PrintWriter( outputFileName, "UTF-8")
		} 
		else {
		  if(!automaticMappingsGeneration) { new StringWriter }
		  else { 
		    val outputFileName = morphProperties.databaseName + "-result.nt"; 
		    new PrintWriter( outputFileName, "UTF-8");
		  }
		   
		}
		//BUILDING MATERIALIZER
		val materializer = this.buildMaterializer(morphProperties, mappingDocument
		    , outputStream);
		
		//BUILDING DATA TRANSLATOR
		val dataTranslator = try {
		 Some(this.createDataTranslator(mappingDocument, materializer, unfolder
				 , dataSourceReader, connection, morphProperties)) 
		} catch {
		  case e:Exception => {
		    logger.warn("Error building data translator!");
		    None
		  }
		}
		
		//BUILDING QUERY TRANSLATOR
		logger.info("Building query translator...");
		val queryTranslatorFactoryClassName = 
		  morphProperties.queryTranslatorFactoryClassName;
		val queryTranslator = try {
			  val qtAux = this.buildQueryTranslator(queryTranslatorFactoryClassName
					  , mappingDocument, connection, morphProperties);
			  Some(qtAux);
		  } catch {
		    case e:Exception => {
		      logger.warn("Error building query translator!" + e.getMessage());
		    }
		    None
		  }

		//BUILDING QUERY RESULT WRITER
		val queryResultWriter = if(queryTranslator.isDefined) {
			val queryResultWriterFactoryClassName = 
			  morphProperties.queryResultWriterFactoryClassName;
//			val outputStream2 = if(properties.outputFilePath.isDefined) {
//			  new FileOutputStream(properties.outputFilePath.get)
//			} else { new ByteArrayOutputStream() }
			
			val qrwAux = this.buildQueryResultWriter(queryResultWriterFactoryClassName
			    , queryTranslator.get, outputStream);
			Some(qrwAux)
		} else { None }

		//BUILDING RESULT PROCESSOR
		val resultProcessor = if(queryResultWriter.isDefined) {
			val resultProcessorAux = this.buildQueryResultTranslator(dataSourceReader
					, queryResultWriter.get);
			Some(resultProcessorAux)
		} else { None }
		
		val runner = this.createRunner(mappingDocument
//		    , dataSourceReader
		    , unfolder
		    , dataTranslator
//		    , materializer
		    , queryTranslator
		    ,  resultProcessor
		    , outputStream
		)

		runner.ontologyFilePath = morphProperties.ontologyFilePath;
		if(morphProperties.queryFilePath.isDefined) {
			runner.sparqlQuery = Some(QueryFactory.read(morphProperties.queryFilePath.get))
		}
		
//		val mapper = new R2RMLMapper();
//		mapper.run(properties);
		
		runner;
	}
	
	def createRunner(mappingDocument:MorphBaseMappingDocument
//    , dataSourceReader:MorphBaseDataSourceReader
    , unfolder:MorphBaseUnfolder
    , dataTranslator :Option[MorphBaseDataTranslator]
//    , materializer : MorphBaseMaterializer
    , queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
	, outputStream:Writer
    ) : MorphBaseRunner;
	
	def readMappingDocumentFile(mappingDocumentFile:String
	    ,props:MorphProperties, connection:Connection)
	:MorphBaseMappingDocument
	    
	def createUnfolder(md:MorphBaseMappingDocument, properties:MorphProperties):MorphBaseUnfolder;
	
	def createDataTranslator(md:MorphBaseMappingDocument, materializer:MorphBaseMaterializer
	    , unfolder:MorphBaseUnfolder, dataSourceReader:MorphBaseDataSourceReader
	    , connection:Connection, properties:MorphProperties):MorphBaseDataTranslator;
	
	def createConnection(morphProperties:MorphProperties) : Connection = {
		val connection = if(morphProperties.noOfDatabase > 0) {
			val databaseUser = morphProperties.databaseUser;
			val databaseName = morphProperties.databaseName;
			val databasePassword = morphProperties.databasePassword;
			val databaseDriver = morphProperties.databaseDriver;
			val databaseURL = morphProperties.databaseURL;
			DBUtility.getLocalConnection(databaseUser, databaseName, databasePassword, 
					databaseDriver, databaseURL, "Runner");
		} else {
		  null
		}

		connection;
	}	
	

	def buildQueryTranslator(queryTranslatorFactoryClassName:String
	    , md:MorphBaseMappingDocument, connection:Connection
	    , properties:MorphProperties) : IQueryTranslator = {
		val className = if(queryTranslatorFactoryClassName == null || queryTranslatorFactoryClassName.equals("")) {
			Constants.QUERY_TRANSLATOR_FACTORY_CLASSNAME_DEFAULT;
		} else {
			queryTranslatorFactoryClassName; 
		}

		val queryTranslatorFactory = Class.forName(className).newInstance().asInstanceOf[IQueryTranslatorFactory]; 
		val queryTranslator = queryTranslatorFactory.createQueryTranslator(
		    md, connection, properties);

		//query translation optimizer
		val queryTranslationOptimizer = this.buildQueryTranslationOptimizer();
		val eliminateSelfJoin = properties.selfJoinElimination;
		queryTranslationOptimizer.selfJoinElimination = eliminateSelfJoin;
		val eliminateSubQuery = properties.subQueryElimination;
		queryTranslationOptimizer.subQueryElimination = eliminateSubQuery;
		val transJoinEliminateSubQuery = properties.transJoinSubQueryElimination;
		queryTranslationOptimizer.transJoinSubQueryElimination = transJoinEliminateSubQuery;
		val transSTGEliminateSubQuery = properties.transSTGSubQueryElimination;
		queryTranslationOptimizer.transSTGSubQueryElimination = transSTGEliminateSubQuery;
		val subQueryAsView = properties.subQueryAsView;
		queryTranslationOptimizer.subQueryAsView = subQueryAsView;
		queryTranslator.optimizer = queryTranslationOptimizer;
		logger.debug("query translator = " + queryTranslator);
		
		//sparql query
		val queryFilePath = properties.queryFilePath;
//		queryTranslator.setSPARQLQueryByFile(queryFilePath);
		
		queryTranslator.properties = properties;
		queryTranslator
	}
	
	def buildQueryResultWriter(queryResultWriterFactoryClassName:String
	    , queryTranslator:IQueryTranslator, pOutputStream:Writer)
	: MorphBaseQueryResultWriter = {
		val className = if(queryResultWriterFactoryClassName == null 
				|| queryResultWriterFactoryClassName.equals("")) {
			Constants.QUERY_RESULT_WRITER_FACTORY_CLASSNAME_DEFAULT;
		} else {
			queryResultWriterFactoryClassName; 
		}
		
		val queryResultWriterFactory = Class.forName(className).newInstance().asInstanceOf[QueryResultWriterFactory];
		val queryResultWriter = queryResultWriterFactory.createQueryResultWriter(
		    queryTranslator, pOutputStream);
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
	    , mappingDocument:MorphBaseMappingDocument, outputStream:Writer) 
	: MorphBaseMaterializer = {
//		val outputFileName = configurationProperties.outputFilePath;
		val rdfLanguage = configurationProperties.rdfLanguage;
		val jenaMode = configurationProperties.jenaMode;
		val materializer = MaterializerFactory.create(rdfLanguage, outputStream, jenaMode);
		val mappingDocumentPrefixMap = mappingDocument.mappingDocumentPrefixMap; 
		if(mappingDocumentPrefixMap != null) {
			materializer.setModelPrefixMap(mappingDocumentPrefixMap);
		}
		materializer
	}	
}

