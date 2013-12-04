package es.upm.fi.dia.oeg.obdi.core.engine;

import java.sql.ResultSet;
import java.sql.SQLException;
import es.upm.fi.dia.oeg.obdi.core.exception.ResultSetException;

public class RDBResultSet extends AbstractResultSet {
	private ResultSet rs;
	
	public RDBResultSet(ResultSet rs) {
		super();
		this.rs = rs;
	}
	
	public boolean next() throws ResultSetException {
		try {
			return this.rs.next();
		} catch (SQLException e) {
			return false;
		}
	}

	public Object getObject(int columnIndex) throws ResultSetException {
		try {
			return rs.getObject(columnIndex);
		} catch (SQLException e) {
			throw new ResultSetException(e);
		}
	}

	public Object getObject(String columnLabel) throws ResultSetException {
		try {
			return rs.getObject(columnLabel);
		} catch (SQLException e) {
			throw new ResultSetException(e);
		}
	}

	public String getString(int columnIndex) throws ResultSetException {
		try {
			return rs.getString(columnIndex);
		} catch (SQLException e) {
			throw new ResultSetException(e);
		}
	}

	public String getString(String columnLabel) throws ResultSetException {
		try {
			return rs.getString(columnLabel);
		} catch (SQLException e) {
			throw new ResultSetException(e);
		}
	}

	public Integer getInt(int columnIndex) throws ResultSetException {
		try {
			return rs.getInt(columnIndex);
		} catch (SQLException e) {
			throw new ResultSetException(e);
		}
	}

	public Integer getInt(String columnLabel) throws ResultSetException {
		try {
			return rs.getInt(columnLabel);
		} catch (SQLException e) {
			throw new ResultSetException(e);
		}
	}


}
