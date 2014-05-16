package es.upm.fi.dia.oeg.morph.base.sql

import org.apache.log4j.Logger
import java.sql.Connection
import java.sql.DatabaseMetaData

class MorphDatabaseMetaData(val conn:Connection, val dbName:String, val dbType:String
    , var tablesMetaData : List[MorphTableMetaData], jdbcDBMetaData:DatabaseMetaData) 
    {
	val logger = Logger.getLogger(this.getClass().getName());
	logger.debug("Database MetaData created: " + this.dbName);
	
	def this(conn:Connection, dbName:String, dbType:String) = {
	  this(conn, dbName, dbType, Nil, null)
	}

	def getTableMetaData(tableName:String) : Option[MorphTableMetaData] = {
	  val result = this.tablesMetaData.find(x => {
	    val xTableName = MorphSQLUtility.printWithoutEnclosedCharacters(x.tableName, dbType);
	    val inputTableName = MorphSQLUtility.printWithoutEnclosedCharacters(tableName, dbType);
	    xTableName.equalsIgnoreCase(inputTableName)
	 });
	  
	  result
	}
	
	def addTableMetaData(tableName:String, tableMetaData:MorphTableMetaData) = {
	  this.tablesMetaData = this.tablesMetaData ::: List(tableMetaData);
	}
}

object MorphDatabaseMetaData {
	val logger = Logger.getLogger(this.getClass().getName());
	
 	def apply(conn:Connection 
// 	    , properties:ConfigurationProperties
 	    , databaseName:String, databaseType:String
 	    ) 
 	: MorphDatabaseMetaData = {
 	  logger.debug("Building Database MetaData");
 	  
 	   val jdbcDBMetaData = try { conn.getMetaData(); } 
 	   catch { 
 	     case e:Exception => {
 	    	 logger.error("Error while getting JDBC DatabaseMetaData" + e.getMessage());
 	    	 null 
 	     }
 	   }
 	     
 	   val jdbcDriverName = if(jdbcDBMetaData != null) {jdbcDBMetaData.getDriverName()}
 	   else {null}
 	   
 	   val dbName = {if(databaseName != null) { databaseName}
 	     else { null }
 	   }

 	   val jdbcProductName = if(jdbcDBMetaData != null) {jdbcDBMetaData.getDatabaseProductName()}
 	   else {null}
 	   val dbType = { if(databaseType != null) { databaseType } 
 	   	else { jdbcProductName } 	     
 	   }
 	   
		val dbMetaData : MorphDatabaseMetaData = {
	 		if(conn != null) {
				try {
					val listTableMetaData = MorphTableMetaData.buildTablesMetaData(conn, dbName, dbType);
					val mapColumnsMetaData = MorphColumnMetaDataFactory.buildMapColumnsMetaData(conn, dbName, dbType);
					val commonTableNames = listTableMetaData.map(x => x.tableName).toSet.intersect(mapColumnsMetaData.keySet);
					commonTableNames.foreach(tableName => {
						val tableMetaData = listTableMetaData.find(p => p.tableName.equals(tableName)).get;
						val columnsMetaData = mapColumnsMetaData(tableName);
						tableMetaData.columnsMetaData = columnsMetaData;					  
					})
					new MorphDatabaseMetaData(conn, dbName, dbType, listTableMetaData
					    , jdbcDBMetaData);
				} catch {
				  case e:Exception => {
				    logger.error("Error while building table meta data:" + e.getMessage());
				    null
				  }
				}
	 		} else {
	 		  null
	 		}
		}
		dbMetaData;
	}
 	
 	
}