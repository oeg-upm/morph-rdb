package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.sql.IQuery

abstract class AbstractQueryResultTranslator(dataSourceReader:MorphBaseDataSourceReader 
			, queryResultWriter:MorphBaseQueryResultWriter ) {
  def translateResult(sqlQueries:Iterable[IQuery] );

}