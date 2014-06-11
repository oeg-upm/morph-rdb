package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseQueryResultWriter
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import com.hp.hpl.jena.query.Query
import org.apache.log4j.Logger

class QueryResultTranslator(dataSourceReader:MorphBaseDataSourceReader 
			, queryResultWriter:MorphBaseQueryResultWriter ) 
			extends AbstractQueryResultTranslator(dataSourceReader, queryResultWriter){
	val logger = Logger.getLogger(this.getClass());
	
	def translateResult(mapSparqlSql:Map[Query, IQuery] ) {
		val start = System.currentTimeMillis();
		this.queryResultWriter.initialize();

		var i=0;
		mapSparqlSql.foreach(mapElement => {
			val sparqlQuery = mapElement._1
			val iQuery = mapElement._2
			
			val abstractResultSet = this.dataSourceReader.execute(iQuery.toString());
			val columnNames = iQuery.getSelectItemAliases();
			abstractResultSet.setColumnNames(columnNames);

			this.queryResultWriter.sparqlQuery = sparqlQuery;
			this.queryResultWriter.setResultSet(abstractResultSet);
			if(i==0) {
				this.queryResultWriter.preProcess();	
			}
			this.queryResultWriter.process();
			i = i + 1;		  
		})

		if(i > 0) {
			this.queryResultWriter.postProcess();	
		}
		
		val end = System.currentTimeMillis();
		logger.info("Result generation time = "+ (end-start)+" ms.");
	}

}