package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseQueryResultWriter
import es.upm.fi.dia.oeg.morph.base.engine.QueryResultWriterFactory
import java.io.OutputStream

class XMLQueryResultWriterFactory extends QueryResultWriterFactory{
	
  override def createQueryResultWriter(queryTranslator:IQueryTranslator, outputStream:OutputStream) 
  : MorphBaseQueryResultWriter = {
    if(queryTranslator == null) {
      throw new Exception("Query Translator is not set yet!");
    }
	val result = new MorphXMLQueryResultWriter(queryTranslator, outputStream);
		result
	} 
}