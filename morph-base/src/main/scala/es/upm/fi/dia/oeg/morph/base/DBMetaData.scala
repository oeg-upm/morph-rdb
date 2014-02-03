package es.upm.fi.dia.oeg.morph.base

import org.apache.log4j.Logger
import java.sql.Connection

class DBMetaData(var mapTableMetaData : Map[String, TableMetaData]) {
	def this() = {
	  this(Map.empty)
	}

	def getTableMetaData(tableName:String) : Option[TableMetaData] = {
	  this.mapTableMetaData.get(tableName);
	}
	
	def putTableMetaData(tableName:String, tableMetaData:TableMetaData) = {
	  this.mapTableMetaData += (tableName -> tableMetaData);
	}
}

object DBMetaData {
	val logger = Logger.getLogger("TableMetaData");
	
 	def buildTablesMetaData(conn:Connection , databaseName:String , databaseType:String ) 
 	: DBMetaData = {
		var mapTableMetaData:Map[String, TableMetaData] = Map.empty;
		
		if(conn != null) {
			try {
				val stmt = conn.createStatement();
				
				var query:String = null;
				var tableNameColumn :String = null;
				var tableRowsColumn : String = null;
				if(databaseType.equalsIgnoreCase(Constants.DATABASE_MYSQL)) {
					query = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA = '" + databaseName + "'";
					tableNameColumn = "TABLE_NAME";
					tableRowsColumn = "TABLE_ROWS";
				} else if(databaseType.equalsIgnoreCase(Constants.DATABASE_POSTGRESQL)) {
					query = "SELECT * FROM pg_stat_user_tables ";
					tableNameColumn = "relname";
					tableRowsColumn = "seq_tup_read";					
				}

				if(query != null) {
					val rs = stmt.executeQuery(query);
					while(rs.next()) {
						val tableName = rs.getString(tableNameColumn);
						val tableRows = rs.getLong(tableRowsColumn);
						val tableMetaData = new TableMetaData(tableName, tableRows);
						mapTableMetaData += (tableMetaData.tableName -> tableMetaData);
					}					
				}
			} catch {
			  case e:Exception => {
			    logger.error("Error while getting table meta data");
			  }
			}
		}
		
		val dbMetaData = new DBMetaData(mapTableMetaData);
		dbMetaData;
	}  
}