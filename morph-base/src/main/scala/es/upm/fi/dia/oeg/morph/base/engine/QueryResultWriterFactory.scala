package es.upm.fi.dia.oeg.morph.base.engine

abstract class QueryResultWriterFactory {
	def createQueryResultWriter(queryTranslator:IQueryTranslator) : MorphBaseQueryResultWriter ;
}