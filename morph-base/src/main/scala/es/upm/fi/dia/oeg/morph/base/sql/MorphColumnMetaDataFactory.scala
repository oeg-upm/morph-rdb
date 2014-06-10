package es.upm.fi.dia.oeg.morph.base.sql

import java.sql.Connection
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import java.util.HashMap
import es.upm.fi.dia.oeg.morph.base.Constants

object MorphColumnMetaDataFactory {
//	val DATABASE_MONETDB = "MonetDB";
//	val DATABASE_ORACLE = "Oracle";
//	val DATABASE_MYSQL = "MySQL";
//	val DATABASE_MYSQL_ENCLOSED_CHARACTER = "`";
//	val DATABASE_SQLSERVER = "SQLServer";
//	val DATABASE_POSTGRESQL = "PostgreSQL";
//	val DATABASE_POSTGRESQL_ENCLOSED_CHARACTER = "\"";
	
	val logger = Logger.getLogger("ColumnMetaDataFactory");
		
	def buildMapColumnsMetaData(conn : Connection, databaseName : String, databaseType : String) 
		: Map[String, List[MorphColumnMetaData]] = {
		logger.debug("\t\tBuilding Columns MetaData for database: " + databaseName);
		
		//var result = Map.empty[String, Map[String, ColumnMetaData]];
		var result:Map[String, List[MorphColumnMetaData]] = Map.empty;
		
		if(conn != null) {
			try {
				val stmt = conn.createStatement();
				val morphInformationSchema = MorphInformationSchema.apply(databaseType);
				val tableNameColumn = morphInformationSchema.tableNameColumn;
				val columnNameColumn = morphInformationSchema.columnNameColumn;
				val datatypeColumn = morphInformationSchema.datatypeColumn;
				val isNullableColumn = morphInformationSchema.isNullableColumn;
				val characterMaximumLengthColumn = morphInformationSchema.characterMaximumLengthColumn;
				val columnKeyColumn = morphInformationSchema.columnKeyColumn;
				
				val query = {
					if(databaseType.equalsIgnoreCase(Constants.DATABASE_MYSQL)) {
						"SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = '" + databaseName + "'";
					} else if(databaseType.equalsIgnoreCase(Constants.DATABASE_POSTGRESQL)) {
						"SELECT * FROM information_schema.columns";
					} else {
					  null
					}
				}
				
				if(query != null) {
					val rs = stmt.executeQuery(query);
					while(rs.next()) {
						try {
							val tableName = rs.getString(tableNameColumn);
							if(!result.contains(tableName)) {
								result += tableName -> Nil;					  
							}
							var listColumnsMetaData = result(tableName);
							val columnName = rs.getString(columnNameColumn);
	

							
							if(!listColumnsMetaData.exists(x=>x.columnName.equals(columnName))) {
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
								
								val columnKey = if(columnKeyColumn.isDefined) {
									val columnKeyString = rs.getString(columnKeyColumn.get);
									Some(columnKeyString);
								} 
								else { None }
								
								val characterMaximumLength = rs.getLong(characterMaximumLengthColumn);
								val newColumnMetaData = new MorphColumnMetaData(
										tableName, columnName, dataType, isNullable, characterMaximumLength, columnKey);
								listColumnsMetaData = listColumnsMetaData ::: List(newColumnMetaData);
								result += tableName -> listColumnsMetaData;
							}					    
					  	}
						catch {
						  case e:Exception => {
						    val errorMessage = "Error while getting table meta data: " + e.getMessage();
						    logger.error(errorMessage);
						  }
						}
					}
				}
			} 
			catch {
			  case e:Exception => {
			    val errorMessage = "Error while getting table meta data: " + e.getMessage();
			    logger.error(errorMessage);
			  }
			}
		}
		
		return result;
	}
 
}