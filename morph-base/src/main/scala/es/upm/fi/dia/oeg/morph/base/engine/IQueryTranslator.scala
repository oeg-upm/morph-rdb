package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import com.hp.hpl.jena.sparql.algebra.Op

trait IQueryTranslator {
	var connection:Connection = null;

//	var sparqlQuery :Query = null;
	
	var optimizer:QueryTranslationOptimizer  = null;
	
	var properties:MorphProperties =null;

	var databaseType:String =null;

	//val unfolder:MorphBaseUnfolder=null;

	var mappingDocument:MorphBaseMappingDocument= null;

//	def setSPARQLQueryByString(queryString:String );
//	
//	def setSPARQLQueryByFile(queryFilePath:String );
	
	def getTranslationResult():IQuery ;
	
	def translate(query:Query) :IQuery ;

	def translate(op:Op) :IQuery ;
	
	def translateFromQueryFile(queryFilePath:String ) : IQuery;

	//IQueryTranslationOptimizer getOptimizer();

	def translateFromString(queryString:String ) : IQuery; 

	//String translateResultSet(String columnLabel, String dbValue);
	
	def translateResultSet(varName:String , rs:MorphBaseResultSet ):TermMapResult ;
	
	def setDatabaseType(dbType:String) = {this.databaseType = dbType}

	def trans(op:Op ) : IQuery;
	
	
}