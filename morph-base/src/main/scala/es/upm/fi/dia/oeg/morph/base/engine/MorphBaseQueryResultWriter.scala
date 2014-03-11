package es.upm.fi.dia.oeg.morph.base.engine

import java.io.OutputStream

abstract class MorphBaseQueryResultWriter(queryTranslator:IQueryTranslator, outputStream:OutputStream) {
	var resultSet:MorphBaseResultSet=null
	def initialize();
	def preProcess();
	def process();
	def postProcess() ;
	def getOutput():Object ;
	def setResultSet(resultSet:MorphBaseResultSet) = {this.resultSet = resultSet}
	
}