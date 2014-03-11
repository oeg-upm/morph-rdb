package es.upm.fi.dia.oeg.morph.base.engine

import java.io.OutputStream

abstract class QueryResultWriterFactory {
	def createQueryResultWriter(queryTranslator:IQueryTranslator, outputStream:OutputStream) 
	: MorphBaseQueryResultWriter ;
}