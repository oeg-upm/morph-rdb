package es.upm.fi.dia.oeg.obdi.core.engine;

public abstract class AbstractDataSourceReader {
	public abstract AbstractResultSet evaluateQuery(String query) throws Exception;
}
