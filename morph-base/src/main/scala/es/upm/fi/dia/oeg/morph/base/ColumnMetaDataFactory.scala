package es.upm.fi.dia.oeg.morph.base

import java.sql.Connection
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import java.util.HashMap

object ColumnMetaDataFactory {
	val DATABASE_MONETDB = "MonetDB";
	val DATABASE_ORACLE = "Oracle";
	val DATABASE_MYSQL = "MySQL";
	val DATABASE_MYSQL_ENCLOSED_CHARACTER = "`";
	val DATABASE_SQLSERVER = "SQLServer";
	val DATABASE_POSTGRESQL = "PostgreSQL";
	val DATABASE_POSTGRESQL_ENCLOSED_CHARACTER = "\"";
	
	val logger = Logger.getLogger("ColumnMetaDataFactory");
		
	def buildColumnsMetaData(conn : Connection, databaseName : String, databaseType : String) 
		: java.util.Map[String, java.util.Map[String, ColumnMetaData]] = {
		//var result = Map.empty[String, Map[String, ColumnMetaData]];
		var result = new java.util.HashMap[String, java.util.Map[String, ColumnMetaData]]();
		
		if(conn != null) {
			try {
				val stmt = conn.createStatement();
				
				var query : String = null;
				var tableNameColumn : String = null;
				var columnNameColumn : String = null;
				var datatypeColumn : String = null;
				var isNullableColumn : String = null;
				
				if(databaseType.equalsIgnoreCase(this.DATABASE_MYSQL)) {
					query = "SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = '" + databaseName + "'";
					tableNameColumn = "TABLE_NAME";
					columnNameColumn = "COLUMN_NAME";
					datatypeColumn = "DATA_TYPE";
					isNullableColumn = "IS_NULLABLE";
				} else if(databaseType.equalsIgnoreCase(this.DATABASE_POSTGRESQL)) {
					query = "SELECT * FROM information_schema.columns";
					tableNameColumn = "table_name";
					columnNameColumn = "column_name";
					datatypeColumn = "data_type";
					isNullableColumn = "is_nullable";					
				}
				
				val rs = stmt.executeQuery(query);
				while(rs.next()) {
					val tableName = rs.getString(tableNameColumn);
					if(!result.contains(tableName)) {
						val emptyMap = Map.empty[String, ColumnMetaData];
						result += tableName -> emptyMap;					  
					}
					var mapColumnMetaData = result(tableName).toMap;
					
					val columnName = rs.getString(columnNameColumn);
					if(!mapColumnMetaData.contains(columnName)) {
						val dataType = rs.getString(datatypeColumn);
						val isNullable =  {
							val isNullableString = rs.getString(isNullableColumn);
							if(isNullableString == null) {
								true;
							} else {
								if(isNullableString.equalsIgnoreCase("NO") 
								    || isNullableString.equalsIgnoreCase("FALSE")) {
									false;
								} else {
								  true
								}
							}							
						}
						
						val columnMetaData = new ColumnMetaData(
								tableName, columnName, dataType, isNullable);
						mapColumnMetaData += (columnName -> columnMetaData);
						result += (tableName -> mapColumnMetaData);
					}
				}
			} catch {
			  case e:Exception => {
			    val errorMessage = "Error while getting table meta data: " + e.getMessage();
			    logger.error(errorMessage);
			  }
			}
		}
		
		return result;
	}
 
}