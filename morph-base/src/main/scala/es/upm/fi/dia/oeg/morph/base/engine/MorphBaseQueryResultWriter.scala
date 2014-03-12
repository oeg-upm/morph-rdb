package es.upm.fi.dia.oeg.morph.base.engine

import java.io.OutputStream
import java.io.Writer
import com.hp.hpl.jena.query.Query

abstract class MorphBaseQueryResultWriter(queryTranslator:IQueryTranslator
    , var outputStream:Writer) {
	
	var sparqlQuery:Query=null;
	var resultSet:MorphBaseResultSet=null
	
	
	def initialize();
	def preProcess();
	def process();
	def postProcess() ;
	def getOutput():Object ;
	def setResultSet(resultSet:MorphBaseResultSet) = {this.resultSet = resultSet}
	
}