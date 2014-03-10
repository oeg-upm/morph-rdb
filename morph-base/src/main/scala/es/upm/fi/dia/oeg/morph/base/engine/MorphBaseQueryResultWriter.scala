package es.upm.fi.dia.oeg.morph.base.engine

abstract class MorphBaseQueryResultWriter(queryTranslator:IQueryTranslator) {
  
	var resultSet:MorphBaseResultSet=null
	def initialize();
	def preProcess();
	def process();
	def postProcess() ;
	def getOutput():Object ;
	def setOutput(output:Object ) ;
	def setResultSet(resultSet:MorphBaseResultSet) = {this.resultSet = resultSet}
	
}