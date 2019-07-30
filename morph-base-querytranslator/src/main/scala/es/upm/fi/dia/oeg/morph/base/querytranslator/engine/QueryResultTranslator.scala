package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseQueryResultWriter
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import org.apache.jena.query.Query
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class QueryResultTranslator(dataSourceReader:MorphBaseDataSourceReader
														, queryResultWriter:MorphBaseQueryResultWriter )
	extends AbstractQueryResultTranslator(dataSourceReader, queryResultWriter){
	val logger = LoggerFactory.getLogger(this.getClass());

	def translateResult(mapSparqlSql:Map[Query, IQuery] ) {
		val start = System.currentTimeMillis();
		this.queryResultWriter.initialize();

		var i=0;
		mapSparqlSql.foreach(mapElement => {
			val sparqlQuery = mapElement._1
			val iQuery = mapElement._2

			val iQueryString = iQuery.toString();
			val rs = this.dataSourceReader.execute(iQueryString);
			val columnNames = iQuery.getSelectItemAliases();
			//abstractResultSet.setColumnNames(columnNames);
			//val rsColumnNames = rs.getColumnNames();
			val rsmd = rs.getMetaData
			val columnNamesListBuffer = new ListBuffer[String]()
			for(i <- 0 to rsmd.getColumnCount-1) {
				//columnNamesListBuffer += rsmd.getColumnName(i+1)
				columnNamesListBuffer += rsmd.getColumnLabel(i+1)
			}
			val rsColumnNames = columnNamesListBuffer.toList

			this.queryResultWriter.sparqlQuery = sparqlQuery;
			this.queryResultWriter.setResultSet(rs);
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