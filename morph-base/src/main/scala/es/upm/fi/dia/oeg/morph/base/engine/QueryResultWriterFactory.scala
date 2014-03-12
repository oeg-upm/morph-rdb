package es.upm.fi.dia.oeg.morph.base.engine

import java.io.OutputStream
import java.io.Writer

abstract class QueryResultWriterFactory {
	def createQueryResultWriter(queryTranslator:IQueryTranslator, outputStream:Writer) 
	: MorphBaseQueryResultWriter ;
}