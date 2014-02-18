package es.upm.fi.dia.oeg.morph.base.engine

abstract class AbstractDataSourceReader {
	def evaluateQuery(query:String): AbstractResultSet;

}