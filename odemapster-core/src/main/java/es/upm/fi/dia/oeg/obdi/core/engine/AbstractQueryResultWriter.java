package es.upm.fi.dia.oeg.obdi.core.engine;

import com.hp.hpl.jena.query.Query;

public abstract class AbstractQueryResultWriter {
	private AbstractResultSet abstractResultSet;
	private IQueryTranslator queryTranslator;
	//Query sparqQuery;
	
	public abstract void initialize() throws Exception;
	public abstract void preProcess() throws Exception;
	public abstract void process() throws Exception;
	public abstract void postProcess() throws Exception;
	public abstract Object getOutput() throws Exception;
	public abstract void setOutput(Object output) throws Exception;
	
	public void setResultSet(AbstractResultSet resultSet) {
		this.abstractResultSet = resultSet;
	}
	void setQueryTranslator(IQueryTranslator queryTranslator) {
		this.queryTranslator = queryTranslator;
	}
	public IQueryTranslator getQueryTranslator() {
		return queryTranslator;
	}
	public AbstractResultSet getResultSet() {
		return abstractResultSet;
	}

//	public Query getSparqQuery() {
//		return sparqQuery;
//	}
//	void setSparqQuery(Query sparqQuery) {
//		this.sparqQuery = sparqQuery;
//	}

}
