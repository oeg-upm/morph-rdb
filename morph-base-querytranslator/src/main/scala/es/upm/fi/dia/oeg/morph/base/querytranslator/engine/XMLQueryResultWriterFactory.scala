package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseQueryResultWriter
import es.upm.fi.dia.oeg.morph.base.engine.QueryResultWriterFactory

class XMLQueryResultWriterFactory extends QueryResultWriterFactory{
	
  override def createQueryResultWriter(queryTranslator:IQueryTranslator) 
  : MorphBaseQueryResultWriter = {
    if(queryTranslator == null) {
      throw new Exception("Query Translator is not set yet!");
    }
	val result = new MorphXMLQueryResultWriter(queryTranslator);
		result
	} 
}