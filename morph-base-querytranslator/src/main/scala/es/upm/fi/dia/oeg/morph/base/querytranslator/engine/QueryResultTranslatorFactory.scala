package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseQueryResultWriter
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslatorFactory

class DefaultQueryResultTranslatorFactory extends AbstractQueryResultTranslatorFactory {
	def createQueryResultTranslator(dataSourceReader:MorphBaseDataSourceReader 
			, queryResultWriter:MorphBaseQueryResultWriter ) = {
	  new QueryResultTranslator(dataSourceReader, queryResultWriter);
	}
}