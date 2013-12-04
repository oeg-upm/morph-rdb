package es.upm.fi.dia.oeg.obdi.core.engine;

import java.util.LinkedList;

import es.upm.fi.dia.oeg.obdi.core.exception.ResultSetException;

public abstract class AbstractResultSet {
	private LinkedList<String> columnNames = null;

	public abstract boolean next() throws ResultSetException;
//	public Object getObject(int columnIndex) throws ResultSetException;
//	public Object getObject(String columnLabel) throws ResultSetException;
	public abstract String getString(int columnIndex) throws ResultSetException;
	public abstract String getString(String columnLabel) throws ResultSetException;
	public abstract Integer getInt(int columnIndex) throws ResultSetException;
	public abstract Integer getInt(String columnLabel) throws ResultSetException;

	public LinkedList<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(LinkedList<String> columnNames) {
		this.columnNames = columnNames;
	}
	
	public String getColumnName(int columnIndex) {
		return this.columnNames.get(columnIndex);
	}
}
