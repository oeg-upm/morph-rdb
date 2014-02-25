package es.upm.fi.dia.oeg.morph.base.sql

import org.apache.log4j.Logger
import java.sql.Connection
import java.sql.DatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties

class MorphDatabaseMetaData(val conn:Connection, val dbName:String, val dbType:String
    , var tablesMetaData : List[MorphTableMetaData], jdbcDBMetaData:DatabaseMetaData) 
    {
  
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
	val logger = Logger.getLogger("TableMetaData");
	
 	def apply(conn:Connection , properties:ConfigurationProperties) 
 	: MorphDatabaseMetaData = {
 	   val jdbcDBMetaData = try {
 	     conn.getMetaData();
 	   } catch {
 	     case e:Exception => {
 	    	 logger.error("Error while getting JDBC DatabaseMetaData");
 	    	 null 	       
 	     }
 	   }
 	   
 	   val productName = jdbcDBMetaData.getDatabaseProductName()
 	   val driverName = jdbcDBMetaData.getDriverName()
 	   
 	   
 	   val dbName = {
 	     if(properties != null) { properties.databaseName}
 	     else { null }
 	   }
 	   
 	   val dbType = {
 	     if(properties != null) { 
 	       if(properties.databaseType != null) {
 	         properties.databaseType
 	       } else {
 	    	   if(jdbcDBMetaData != null) { jdbcDBMetaData.getDatabaseProductName() }
 	    	   else { null } 	         
 	       }
 	     } else {
 	       if(jdbcDBMetaData != null) { jdbcDBMetaData.getDatabaseProductName() }
 	       else { null }  
 	     } 	     
 	   }
 	   
		val dbMetaData : MorphDatabaseMetaData = {
	 		if(conn != null) {
				try {
					val listTableMetaData = MorphTableMetaData.buildTablesMetaData(conn, dbName, dbType);
					val mapColumnsMetaData = MorphColumnMetaDataFactory.buildMapColumnsMetaData(conn, dbName, dbType);
					val commonTableNames = listTableMetaData.map(x => x.tableName).toSet.intersect(mapColumnsMetaData.keySet);
					for(tableName <- commonTableNames) {
						val tableMetaData = listTableMetaData.find(p => p.tableName.equals(tableName)).get;
						val columnsMetaData = mapColumnsMetaData(tableName);
						tableMetaData.columnsMetaData = columnsMetaData;
					}
					new MorphDatabaseMetaData(conn, dbName, dbType, listTableMetaData
					    , jdbcDBMetaData);
				} catch {
				  case e:Exception => {
				    logger.error("Error while building table meta data");
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