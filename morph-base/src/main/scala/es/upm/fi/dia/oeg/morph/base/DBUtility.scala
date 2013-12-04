package es.upm.fi.dia.oeg.morph.base

import org.apache.log4j.Logger
import java.sql.Connection
import java.sql.SQLException
import java.sql.ResultSet
import java.sql.Statement
import java.util.Properties
import java.sql.DriverManager

class DBUtility {

}

object DBUtility {
	def logger = Logger.getLogger("DBUtility");
	
	
	def execute(conn : Connection , query: String) : Boolean = {
		val stmt = conn.createStatement();

		try  {
			stmt.setQueryTimeout(60);
			stmt.setFetchSize(Integer.MIN_VALUE);
		} catch {
		  case e:Exception => {
		    logger.debug("Can't set fetch size!");
		  }
		}

		logger.debug("Executing query = \n" + query);

		try {
			val start = System.currentTimeMillis();
			val result = stmt.execute(query);
			val end = System.currentTimeMillis();
			logger.debug("View creation/deletion time was "+(end-start)+" ms.");
			return result;
		} catch {
		  case e:SQLException => {
			e.printStackTrace();
			logger.error("Error executing query, error message = "+ e.getMessage());
			false;
		  }
		}

	}

	def executeQuery(conn:Connection , query:String , timeout:Integer) : ResultSet = {
		try {
			val stmt = conn.createStatement(
					java.sql.ResultSet.TYPE_FORWARD_ONLY,
					java.sql.ResultSet.CONCUR_READ_ONLY);
			
			val dbmd = conn.getMetaData();
			val dbProductName = dbmd.getDatabaseProductName();
			if(Constants.DATABASE_MYSQL.equalsIgnoreCase(dbProductName)) {
				stmt.setFetchSize(Integer.MIN_VALUE);	
			}
			
			if(timeout > 0) {
				stmt.setQueryTimeout(timeout);
			}
			
			val start = System.currentTimeMillis();
			val result = stmt.executeQuery(query);
			val end = System.currentTimeMillis();
			logger.info("SQL execution time was "+(end-start)+" ms.");
			result;
		} catch {
			case e:Exception => {
				logger.error("Error creating statement object, error message = "+ e.getMessage());
				null		    
			}
		}
	}

	def closeConnection(conn:Connection , requesterString:String) = {
		try {
			if(conn != null) {
				conn.close();
				logger.info("Closing db connection.");
			}
		} catch {
		  case e:Exception => {
		    logger.error("Error closing connection! Error message = " + e.getMessage());
		  }
		}
	}

	def closeRecordSet(rs:ResultSet ) = {
		try {
			if(rs != null) {
				rs.close();
			}
		} catch {
		  case e:Exception => {
		    logger.error("Error closing result set! Error message = " + e.getMessage());
		  }
		}
	}

	def closeStatement(stmt:Statement ) = {
		try {
			if(stmt != null) {
				stmt.close();
			}
		} catch {
		  case e:Exception =>{
		    logger.error("Error closing statement! Error message = " + e.getMessage());
		  } 
		}
	}

	def getRowCount(set:ResultSet ) : Integer = {
		val currentRow = set.getRow();            // Get current row
		
		val rowCount = {
		  if(set.last()) {
		    set.getRow()
		  } else {
		    0
		  }
		}  
		  
		if (currentRow == 0)                      // If there was no current row  
			set.beforeFirst();                     // We want next() to go to first row  
		else                                      // If there WAS a current row  
			set.absolute(currentRow);              // Restore it  
		return rowCount;  
	}

	def  getLocalConnection(username:String, databaseName:String, password:String
	    , driverString:String, url:String, requester:String) : Connection = {

		try {
			val prop = new Properties();
			prop.put("ResultSetMetaDataOptions", "1");
			prop.put("user", username);
			prop.put("database", databaseName);
			prop.put("password", password);
			prop.put("autoReconnect", "true");
			Class.forName(driverString);
			logger.debug("Opening database connection.");
			DriverManager.getConnection(url, prop);
		} catch {
		  case e:ClassNotFoundException => {
			val errorMessage = "Error opening database connection, class not found : " + e.getMessage();
			logger.error(errorMessage);
			null
		  }
		  case e:Exception => {
		    logger.error("Error opening database connection : " + e.getMessage());
		    null
		  }
		}
		
	}  
}