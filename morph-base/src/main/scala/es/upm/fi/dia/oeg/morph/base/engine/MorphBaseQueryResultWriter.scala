package es.upm.fi.dia.oeg.morph.base.engine

import java.io.OutputStream
import java.io.Writer
import java.sql.ResultSet

import org.apache.jena.query.Query;

abstract class MorphBaseQueryResultWriter(queryTranslator:IQueryTranslator
																					, var outputStream:Writer) {

	var sparqlQuery:Query=null;
	var rs:ResultSet=null


	def initialize();
	def preProcess();
	def process();
	def postProcess() ;
	def getOutput():Object ;
	def setResultSet(rs:ResultSet) = {this.rs = rs}

}