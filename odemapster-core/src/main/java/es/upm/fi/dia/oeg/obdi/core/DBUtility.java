package es.upm.fi.dia.oeg.obdi.core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import Zql.ZSelectItem;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem;


public class DBUtility {
	private static Logger logger = Logger.getLogger(DBUtility.class);
	
	
	public static boolean execute(Connection conn, String query) throws SQLException {
		Statement stmt = conn.createStatement();

		try  {
			stmt.setQueryTimeout(60);
			stmt.setFetchSize(Integer.MIN_VALUE);
		} catch(Exception e) {
			logger.debug("Can't set fetch size!");
		}

		logger.debug("Executing query = \n" + query);

		try {
			long start = System.currentTimeMillis();
			boolean result = stmt.execute(query);
			long end = System.currentTimeMillis();
			logger.debug("View creation/deletion time was "+(end-start)+" ms.");

			return result;
		} catch(SQLException e) {
			e.printStackTrace();
			logger.error("Error executing query, error message = "+ e.getMessage());
			throw e;
		}

	}

	public static ResultSet executeQuery(Connection conn, String query, int timeout) throws Exception {
		//		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		//				ResultSet.CONCUR_READ_ONLY);

		//st.setFetchSize(1000);
		//		Statement st = conn.createStatement();
		
		Statement stmt = null;
		try {
			stmt = conn.createStatement(
					java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			
			DatabaseMetaData dbmd = conn.getMetaData();
			String dbProductName = dbmd.getDatabaseProductName();
			if(Constants.DATABASE_MYSQL().equalsIgnoreCase(dbProductName)) {
				stmt.setFetchSize(Integer.MIN_VALUE);	
			}
		} catch(Exception e) {
			//e.printStackTrace();
			logger.error("Error creating statement object, error message = "+ e.getMessage());
			throw e;
		}


		//Statement stmt = conn.createStatement();//outofmemory error:Java heap space

		try  {
			if(timeout > 0) {
				stmt.setQueryTimeout(timeout);
			}
			//stmt.setFetchSize(Integer.MIN_VALUE);
		} catch(Exception e) {
			logger.warn("Exception occur : " + e.getMessage());
		}

		logger.info("Evaluating query : \n" + query);
		//logger.info("Evaluating query ...");

		try {
			long start = System.currentTimeMillis();
			ResultSet result = stmt.executeQuery(query);
			long end = System.currentTimeMillis();
			logger.info("SQL execution time was "+(end-start)+" ms.");

			return result;
		} catch(SQLException e) {
			//e.printStackTrace();
			logger.error("Error executing query, error message = "+ e.getMessage());
			throw e;
		}

	}

	public static void closeConnection(Connection conn, String requester) {
		try {
			if(conn != null) {
				conn.close();
				logger.info("Closing db connection.");
			}
		} catch(Exception e) {
			logger.error("Error closing connection! Error message = " + e.getMessage());
		}
	}

	public static void closeRecordSet(ResultSet rs) {
		try {
			if(rs != null) {
				rs.close();
			}
		} catch(Exception e) {
			logger.error("Error closing result set! Error message = " + e.getMessage());
		}
	}

	public static void closeStatement(Statement stmt) {
		try {
			if(stmt != null) {
				stmt.close();
			}
		} catch(Exception e) {
			logger.error("Error closing statement! Error message = " + e.getMessage());
		}
	}

	public static int getRowCount(ResultSet set) throws SQLException  
	{  
		int rowCount;  
		int currentRow = set.getRow();            // Get current row  
		rowCount = set.last() ? set.getRow() : 0; // Determine number of rows  
		if (currentRow == 0)                      // If there was no current row  
			set.beforeFirst();                     // We want next() to go to first row  
		else                                      // If there WAS a current row  
			set.absolute(currentRow);              // Restore it  
		return rowCount;  
	}

	public static String getValueWithoutAlias(ZSelectItem selectItem) {
		String result;

		String selectItemString = selectItem.toString();
		String alias = selectItem.getAlias();
		if(alias == null) {
			result = selectItemString;
		} else {
			selectItem.setAlias("");
			result = selectItem.toString();
			selectItem.setAlias(alias);
		}

		if(selectItem instanceof MorphSQLSelectItem) {
			MorphSQLSelectItem sqlSelectItem = (MorphSQLSelectItem) selectItem;
			String columnType = sqlSelectItem.columnType();
			result = result.replaceAll("::" + columnType, "");
		}

		return result.trim();
	}

	public static Connection getLocalConnection(
			String username, String databaseName, String password, String driverString, String url, String requester) 
					throws SQLException {

		try {
			Properties prop = new Properties();
			prop.put("ResultSetMetaDataOptions", "1");
			prop.put("user", username);
			prop.put("database", databaseName);
			prop.put("password", password);
			prop.put("autoReconnect", "true");
			Class.forName(driverString);
			logger.debug("Opening database connection.");
			return DriverManager.getConnection(url, prop);
		} catch(ClassNotFoundException e) {
			String errorMessage = "Error opening database connection, class not found : " + e.getMessage();
			logger.error(errorMessage);
			throw new SQLException(e.getMessage(), e);
		} catch (Exception e) {
			logger.error("Error opening database connection : " + e.getMessage());
			//e.printStackTrace();

			throw new SQLException(e.getMessage(), e);
		}		
	}
}
