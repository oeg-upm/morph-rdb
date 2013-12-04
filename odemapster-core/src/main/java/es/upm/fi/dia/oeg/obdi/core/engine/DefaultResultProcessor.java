package es.upm.fi.dia.oeg.obdi.core.engine;

//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;

import es.upm.fi.dia.oeg.obdi.core.sql.IQuery;

public class DefaultResultProcessor {
	//AbstractRunner runner;
	AbstractDataSourceReader dataSourceReader;
	AbstractQueryResultWriter queryResultWriter;

	public DefaultResultProcessor(AbstractDataSourceReader dataSourceReader, AbstractQueryResultWriter queryResultWriter) {
		this.dataSourceReader = dataSourceReader;
		this.queryResultWriter = queryResultWriter;
	}

	public void translateResult(Collection<IQuery> sqlQueries) throws Exception {
		this.queryResultWriter.initialize();

		int i=0;
		for(IQuery iQuery : sqlQueries) {
			AbstractResultSet abstractResultSet = 
					this.dataSourceReader.evaluateQuery(iQuery.toString());
			LinkedList<String> columnNames = iQuery.getSelectItemAliases();
			abstractResultSet.setColumnNames(columnNames);

			this.queryResultWriter.setResultSet(abstractResultSet);
			if(i==0) {
				this.queryResultWriter.preProcess();	
			}
			this.queryResultWriter.process();
			i++;
		}

		if(i > 0) {
			this.queryResultWriter.postProcess();	
		}
	}

}
