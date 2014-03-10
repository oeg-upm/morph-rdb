package es.upm.fi.dia.oeg.morph.base.engine

import scala.collection.JavaConversions._

import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.materializer.MaterializerFactory
import com.hp.hpl.jena.query.QueryFactory
import es.upm.fi.dia.oeg.newrqr.RewriterWrapper

abstract class MorphBaseRunner(mappingDocument:MorphBaseMappingDocument
    //, conn:Connection
    , dataSourceReader:MorphBaseDataSourceReader
    , unfolder:MorphBaseUnfolder
    , dataTranslator :MorphBaseDataTranslator
    , materializer : MorphBaseMaterializer
    , val queryTranslator:Option[IQueryTranslator]
    , resultProcessor:Option[AbstractQueryResultTranslator]
    //, queryResultWriter :MorphBaseQueryResultWriter
    ) {
  
	var ontologyFilePath:String=null;
	val logger = Logger.getLogger(this.getClass());
//	var configurationProperties:ConfigurationProperties =null;
//	var conn:Connection =null;
//	var dataTranslator :MorphBaseDataTranslator = null;
//	var materializer : MorphBaseMaterializer = null;
	//	private Collection<IQuery> sqlQueries;
	//private String queryResultWriterClassName = null;
//	var queryResultWriterOutput :Object = null;
		



	



	











	def postMaterialize() = {
		//CLEANING UP
		try {
			this.dataTranslator.materializer.postMaterialize();
			//out.flush(); out.close();
			//fileOut.flush(); fileOut.close();
			this.dataSourceReader.closeConnection;
			
		} catch { case e:Exception => { e.printStackTrace(); } } 
	}
	
	def materializeMappingDocuments(md:MorphBaseMappingDocument ) {
		val start = System.currentTimeMillis();
		
		//PREMATERIALIZE PROCESS
//		this.preMaterializeProcess(outputFileName);

		//MATERIALIZING MODEL
		val startGeneratingModel = System.currentTimeMillis();
//		this.dataTranslator.translateData(md);
		val cms = md.classMappings;
		
		//this.dataTranslator.translateData(cms);
		cms.foreach(cm => {
			val sqlQuery = this.unfolder.unfoldConceptMapping(cm);
			this.dataTranslator.generateRDFTriples(cm, sqlQuery);
		})
		
		this.dataTranslator.materializer.materialize();

		//POSTMATERIALIZE PROCESS
		this.postMaterialize();

		val endGeneratingModel = System.currentTimeMillis();
		val durationGeneratingModel = (endGeneratingModel-startGeneratingModel) / 1000;
		logger.info("Materializing Mapping Document time was "+(durationGeneratingModel)+" s.");
	}

	def readSPARQLFile(sparqQueryFileURL:String ) {
		if(this.queryTranslator.isDefined) {
			val sparqQuery = QueryFactory.read(sparqQueryFileURL);
			this.queryTranslator.get.sparqlQuery = sparqQuery;
		}
	}
	
	def materializeSubjects(classURI:String , outputFileName:String ) ={
		val startGeneratingModel = System.currentTimeMillis();
		
		//PREMATERIALIZE PROCESS
//		this.preMaterializeProcess(outputFileName);

		//MATERIALIZING MODEL
		val cms = this.mappingDocument.getConceptMappingsByConceptName(classURI);
		cms.foreach(cm => {
			val sqlQuery = this.unfolder.unfoldConceptMapping(cm);
			this.dataTranslator.generateSubjects(cm, sqlQuery);		  
		})
		this.dataTranslator.materializer.materialize();

		//POSTMATERIALIZE PROCESS
		this.postMaterialize();

		val endGeneratingModel = System.currentTimeMillis();
		val durationGeneratingModel = (endGeneratingModel-startGeneratingModel) / 1000;
		logger.info("Materializing Subjects time was "+(durationGeneratingModel)+" s.");
		//return result;
	}

	def materializeInstanceDetails(subjectURI:String , classURI:String
	    , outputFileName:String ) = {
		val startGeneratingModel = System.currentTimeMillis();
		
		//PREMATERIALIZE PROCESS
//		this.preMaterializeProcess(outputFileName);

		//MATERIALIZING MODEL
		val cms = this.mappingDocument.getConceptMappingsByConceptName(classURI);
		
		cms.foreach(cm => {
			val sqlQuery = this.unfolder.unfoldConceptMapping(cm, subjectURI);
			if(sqlQuery != null) {
				this.dataTranslator.generateRDFTriples(cm, sqlQuery);	
			}		  
		})
		this.dataTranslator.materializer.materialize();

		//POSTMATERIALIZE PROCESS
		this.postMaterialize();

		val endGeneratingModel = System.currentTimeMillis();
		val durationGeneratingModel = (endGeneratingModel-startGeneratingModel) / 1000;
		logger.info("Materializing Subjects time was "+(durationGeneratingModel)+" s.");
	}

	def run() : String = {
		var status:String  = null;

		val sparqlQuery = if(this.queryTranslator.isDefined) {
		  this.queryTranslator.get.sparqlQuery
		} else { null }
		
		if(sparqlQuery == null) {
			//set output file
			this.materializeMappingDocuments(mappingDocument);
		} else {
			logger.debug("sparql query = " + sparqlQuery);

			//LOADING ONTOLOGY FILE
			//REWRITE THE SPARQL QUERY IF NECESSARY
			val queries = if(this.ontologyFilePath == null || this.ontologyFilePath.equals("")) {
				List(sparqlQuery);
			} else {
				//REWRITE THE QUERY BASED ON THE MAPPINGS AND ONTOLOGY
				logger.info("Rewriting query...");
				//				Collection <String> mappedOntologyElements = MappingsExtractor.getMappedPredcatesFromR2O(mappingDocumentFile);
				val mappedOntologyElements = this.mappingDocument.getMappedConcepts();
				val mappedOntologyElements2 = this.mappingDocument.getMappedProperties();
				mappedOntologyElements.addAll(mappedOntologyElements2);


				//RewriterWrapper rewritterWapper = new RewriterWrapper(ontologyFilePath, rewritterWrapperMode, mappedOntologyElements);
				//queries = rewritterWapper.rewrite(originalQuery);
				val queriesAux = RewriterWrapper.rewrite(sparqlQuery, ontologyFilePath
				    , RewriterWrapper.fullMode, mappedOntologyElements
				    , RewriterWrapper.globalMatchMode);

				logger.debug("No of rewriting query result = " + queriesAux.size());
				logger.debug("queries = " + queriesAux);
				queriesAux.toList
			}			


			//TRANSLATE SPARQL QUERIES INTO SQL QUERIES
			val sqlQueries = this.translateSPARQLQueriesIntoSQLQueries(queries);

			//translate result
			//if (this.conn != null) {
			//GFT does not need a Connection instance
			this.resultProcessor.get.translateResult(sqlQueries);	
			//}
		}

		logger.info("**********************DONE****************************");
		return status;

	}

	def translateSPARQLQueriesIntoSQLQueries(sparqlQueries:Iterable[Query] ):Iterable[IQuery]={
		val sqlQueries = sparqlQueries.map(sparqlQuery => {
			logger.debug("SPARQL Query = \n" + sparqlQuery);
			val sqlQuery = this.queryTranslator.get.translate(sparqlQuery);
			logger.debug("SQL Query = \n" + sqlQuery);
			sqlQuery;
		})

		sqlQueries;
	}


}

object MorphBaseRunner {

	


}