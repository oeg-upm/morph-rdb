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
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import java.io.OutputStream
import java.io.Writer
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.sparql.core.BasicPattern
import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import com.hp.hpl.jena.sparql.algebra.op.OpProject
import com.hp.hpl.jena.sparql.algebra.OpAsQuery
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.vocabulary.RDFS
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.Resource

abstract class MorphBaseRunner(mappingDocument:MorphBaseMappingDocument
    //, conn:Connection
    //, dataSourceReader:MorphBaseDataSourceReader
    , unfolder:MorphBaseUnfolder
    , dataTranslator : Option[MorphBaseDataTranslator]
    //, materializer : Option[MorphBaseMaterializer]
    , val queryTranslator:Option[IQueryTranslator]
    , val queryResultTranslator:Option[AbstractQueryResultTranslator]
    , var outputStream:Writer
    //, queryResultWriter :MorphBaseQueryResultWriter
    ) {
  
	val logger = Logger.getLogger(this.getClass());
	var ontologyFilePath:Option[String]=None;
	var sparqlQuery:Option[Query]=None;
	var mapSparqlSql:Map[Query, IQuery] = Map.empty;
	
	def setOutputStream(outputStream:Writer) = { 
		this.outputStream = outputStream
		//	  if(this.materializer.isDefined) {
		//		  this.materializer.get.outputStream = outputStream;
		//	  }
		if(this.dataTranslator.isDefined) {
			this.dataTranslator.get.materializer.outputStream = outputStream; 
		}
		
		if(this.queryResultTranslator.isDefined) {
		  this.queryResultTranslator.get.queryResultWriter.outputStream = outputStream;
		}
	}
	
//	def postMaterialize() = {
//		//CLEANING UP
//		try {
//			this.dataTranslator.materializer.postMaterialize();
//			//out.flush(); out.close();
//			//fileOut.flush(); fileOut.close();
//			this.dataSourceReader.closeConnection;
//			
//		} catch { case e:Exception => { e.printStackTrace(); } } 
//	}
	
	def materializeMappingDocuments(md:MorphBaseMappingDocument ) {
	  if(!this.dataTranslator.isDefined) {
	    val errorMessage = "Data Translator has not been defined yet!";
	    logger.error(errorMessage);
	    throw new Exception(errorMessage)
	  }
	  
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
			this.dataTranslator.get.generateRDFTriples(cm, sqlQuery);
		})
		
		this.dataTranslator.get.materializer.materialize();

		//POSTMATERIALIZE PROCESS
//		this.postMaterialize();

		val endGeneratingModel = System.currentTimeMillis();
		val durationGeneratingModel = (endGeneratingModel-startGeneratingModel) / 1000;
		logger.info("Materializing Mapping Document time was "+(durationGeneratingModel)+" s.");
	}

	def readSPARQLFile(sparqQueryFileURL:String ) {
		if(this.queryTranslator.isDefined) {
			this.sparqlQuery = Some(QueryFactory.read(sparqQueryFileURL));
		}
	}

	def readSPARQLString(sparqString:String ) {
		if(this.queryTranslator.isDefined) {
			this.sparqlQuery = Some(QueryFactory.create(sparqString));
		}
	}


	def run() : String = {
		var status:String  = null;

//		val sparqlQuery = if(this.queryTranslator.isDefined) {
//		  this.queryTranslator.get.sparqlQuery
//		} else { null }
		
		if(!this.sparqlQuery.isDefined) {
			//set output file
			this.materializeMappingDocuments(mappingDocument);
		} else {
			logger.debug("sparql query = " + this.sparqlQuery.get);

			//LOADING ONTOLOGY FILE
			//REWRITE THE SPARQL QUERY IF NECESSARY
			val queries = if(!this.ontologyFilePath.isDefined) {
				List(sparqlQuery.get);
			} else {
				//REWRITE THE QUERY BASED ON THE MAPPINGS AND ONTOLOGY
				logger.info("Rewriting query...");
				//				Collection <String> mappedOntologyElements = MappingsExtractor.getMappedPredcatesFromR2O(mappingDocumentFile);
				val mappedOntologyElements = this.mappingDocument.getMappedClasses();
				val mappedOntologyElements2 = this.mappingDocument.getMappedProperties();
				mappedOntologyElements.addAll(mappedOntologyElements2);


				//RewriterWrapper rewritterWapper = new RewriterWrapper(ontologyFilePath, rewritterWrapperMode, mappedOntologyElements);
				//queries = rewritterWapper.rewrite(originalQuery);
				val queriesAux = RewriterWrapper.rewrite(sparqlQuery.get, ontologyFilePath.get
				    , RewriterWrapper.fullMode, mappedOntologyElements
				    , RewriterWrapper.globalMatchMode);

				logger.debug("No of rewriting query result = " + queriesAux.size());
				logger.debug("queries = " + queriesAux);
				queriesAux.toList
			}			


			//TRANSLATE SPARQL QUERIES INTO SQL QUERIES
			this.mapSparqlSql= this.translateSPARQLQueriesIntoSQLQueries(queries);

			//translate result
			//if (this.conn != null) {
			//GFT does not need a Connection instance
			this.queryResultTranslator.get.translateResult(mapSparqlSql);	
			//}
		}

		logger.info("**********************DONE****************************");
		return status;

	}

	def translateSPARQLQueriesIntoSQLQueries(sparqlQueries:Iterable[Query] )
	:Map[Query, IQuery]={
		val sqlQueries = sparqlQueries.map(sparqlQuery => {
			logger.debug("SPARQL Query = \n" + sparqlQuery);
			val sqlQuery = this.queryTranslator.get.translate(sparqlQuery);
			logger.debug("SQL Query = \n" + sqlQuery);
			(sparqlQuery -> sqlQuery);
		})

		sqlQueries.toMap
	}

	def materializeClassMappings(cms:Iterable[MorphBaseClassMapping]) = {
	  if(!this.dataTranslator.isDefined) {
	    val errorMessage = "Data Translator has not been defined yet!";
	    logger.error(errorMessage);
	    throw new Exception(errorMessage)
	  }

		val startGeneratingModel = System.currentTimeMillis();

	  //PREMATERIALIZE PROCESS
//		this.preMaterializeProcess(outputFileName);

		//MATERIALIZING MODEL
		cms.foreach(cm => {
			val sqlQuery = this.unfolder.unfoldConceptMapping(cm);
			this.dataTranslator.get.generateSubjects(cm, sqlQuery);		  
		})
		this.dataTranslator.get.materializer.materialize();

		//POSTMATERIALIZE PROCESS
//		this.postMaterialize();

		val endGeneratingModel = System.currentTimeMillis();
		val durationGeneratingModel = (endGeneratingModel-startGeneratingModel) / 1000;
		logger.info("Materializing Subjects time was "+(durationGeneratingModel)+" s.");
	}

	def materializeInstanceDetails(subjectURI:String,cms:Iterable[MorphBaseClassMapping]):Unit={
	  if(!this.dataTranslator.isDefined) {
	    val errorMessage = "Data Translator has not been defined yet!";
	    logger.error(errorMessage);
	    throw new Exception(errorMessage)
	  }
	  
		val startGeneratingModel = System.currentTimeMillis();
		
		//PREMATERIALIZE PROCESS
//		this.preMaterializeProcess(outputFileName);

		cms.foreach(cm => {
			val sqlQuery = this.unfolder.unfoldConceptMapping(cm, subjectURI);
			if(sqlQuery != null) {
				this.dataTranslator.get.generateRDFTriples(cm, sqlQuery);	
			}		  
		})
		this.dataTranslator.get.materializer.materialize();

		//POSTMATERIALIZE PROCESS
//		this.postMaterialize();

		val endGeneratingModel = System.currentTimeMillis();
		val durationGeneratingModel = (endGeneratingModel-startGeneratingModel) / 1000;
		logger.info("Materializing Subjects time was "+(durationGeneratingModel)+" s.");
	  
	}
	
	def materializeInstanceDetails(subjectURI:String , classURI:String
	    , outputStream:OutputStream) : Unit = {
		val startGeneratingModel = System.currentTimeMillis();
		
		//PREMATERIALIZE PROCESS
//		this.preMaterializeProcess(outputFileName);

		//MATERIALIZING MODEL
		val cms = this.mappingDocument.getClassMappingsByClassURI(classURI);
		this.materializeInstanceDetails(subjectURI, cms);
	}

	def materializeSubjects(classURI:String) ={
		//MATERIALIZING MODEL
		val cms = this.mappingDocument.getClassMappingsByClassURI(classURI);
		this.materializeClassMappings(cms);
		//return result;
	}
	
	def getQueryTranslator() = {
	  queryTranslator.getOrElse(null);
	}
	
	def getQueryResultWriter() = {
		if(queryResultTranslator.isDefined) {
		  queryResultTranslator.get.queryResultWriter
		}
		else { null }
	}
	
	def getTranslationResults : java.util.Collection[IQuery] = {
	  this.mapSparqlSql.values
	}
	


	
}

