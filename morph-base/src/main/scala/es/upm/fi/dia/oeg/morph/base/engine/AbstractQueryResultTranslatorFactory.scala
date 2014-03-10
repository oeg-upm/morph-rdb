package es.upm.fi.dia.oeg.morph.base.engine

abstract class AbstractQueryResultTranslatorFactory {
	def createQueryResultTranslator(dataSourceReader:MorphBaseDataSourceReader 
			, queryResultWriter:MorphBaseQueryResultWriter ) : AbstractQueryResultTranslator;
}