package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import org.apache.jena.query.Query;

abstract class AbstractQueryResultTranslator(dataSourceReader:MorphBaseDataSourceReader 
			, val queryResultWriter:MorphBaseQueryResultWriter ) {
  def translateResult(sqlQueries:Map[Query, IQuery] );

}