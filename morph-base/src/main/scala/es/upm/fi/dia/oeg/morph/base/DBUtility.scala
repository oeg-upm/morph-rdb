package es.upm.fi.dia.oeg.morph.base

import java.sql.Connection
import java.sql.SQLException
import java.sql.ResultSet
import java.sql.Statement
import java.util.Properties
import java.sql.DriverManager
import org.slf4j.LoggerFactory

class DBUtility {

}

object DBUtility {
	// def logger = LogManager.getLogger(this.getClass());
	val logger = LoggerFactory.getLogger(this.getClass());

	//def logger = Logger.getLogger(this.getClass());



	/*	def execute(conn : Connection , query: String) : Boolean = {
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
    }*/

	def execute(conn:Connection , query:String , timeout:Integer) : ResultSet = {
		logger.info("Executing query ...");
		logger.info("Executing query:\n" + query);

		if(conn == null) {
			val errorMessage = "No connection defined!";
			logger.error(errorMessage);
			throw new Exception(errorMessage);
		}

		val stmt = conn.createStatement(
			java.sql.ResultSet.TYPE_FORWARD_ONLY,
			java.sql.ResultSet.CONCUR_READ_ONLY);

		val dbmd = conn.getMetaData();
		val dbProductName = dbmd.getDatabaseProductName();
		if(Constants.DATABASE_MYSQL.equalsIgnoreCase(dbProductName)) {
			stmt.setFetchSize(Integer.MIN_VALUE);
		}
		else { stmt.setFetchSize(100); }

		if(timeout > 0) { stmt.setQueryTimeout(timeout); }
		val start = System.currentTimeMillis();
		val result = try {
			stmt.execute(query);
		} catch {
			case e:Exception => {
				logger.error("Error while executing SQL Query: " + query);
				throw e;
			}
		}
		//val result = stmt.execute(query);
		val end = System.currentTimeMillis();
		logger.debug("SQL execution time was "+(end-start)+" ms.");

		if(result) { stmt.getResultSet(); }
		else { null }

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

		val result:Connection = try {

			if(driverString.equals("oracle.jdbc.driver.OracleDriver")){
				Class.forName(driverString);
				logger.info("Opening database connection.");

				val fullUrl= {
					if(databaseName == ""){
						url
					}else{
						url.concat(":".concat(databaseName))
					}
				}
				val conn = DriverManager.getConnection(fullUrl,username,password);
				logger.info("Connected to Database: ".concat(url).concat(" with User: ".concat(username)));
				conn;
			}
			else{
				val urlSplit = url.split("/");
				val fullURL = {
					if (databaseName == null) {
						url
					} else {
						if (databaseName.equals(urlSplit(urlSplit.length - 1))) {
							url
						}
						else {
							url + databaseName
						}
					}
				}

				val prop = new Properties();
				prop.put("ResultSetMetaDataOptions", "1");

				if (username != null && !username.equals("")) {
					prop.put("user", username);
				}

				//if(password != null && !password.equals("")) {
				if (password != null) {
					prop.put("password", password);
				}

				if (databaseName != null && !databaseName.equals("")) {
					prop.put("database", databaseName);
				}

				prop.put("autoReconnect", "true");

				prop.put("useSSL", "false");
				prop.put("serverTimezone", "UTC");

				Class.forName(driverString);
				logger.info("Opening database connection ...");
				val conn = DriverManager.getConnection(fullURL, prop);
				//conn.setAutoCommit(true);
				//conn.setAutoCommit(false);
				conn;
			} }catch {
			case e: ClassNotFoundException => {
				//e.printStackTrace();
				//val errorMessage = "Error opening database connection, class not found : " + e.getMessage();
				//logger.error(errorMessage);
				throw e
			}
			case e: Exception => {
				//e.printStackTrace()
				//logger.error("Error opening database connection : " + e.getMessage());
				throw e
			}
		}
		result
	}




}