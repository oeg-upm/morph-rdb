package es.upm.fi.dia.oeg.morph.base.sql

import java.sql.Connection
import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.Constants
import org.slf4j.LoggerFactory


class MorphTableMetaData(val tableName:String , val tableRows:Long
    , var columnsMetaData : List[MorphColumnMetaData], val dbType:String) {
	val logger = LoggerFactory.getLogger(this.getClass());
  logger.debug("\tTable MetaData created: " + this.tableName);

	
	def putColumnMetaData(columnMetaData:MorphColumnMetaData) = {
	  this.columnsMetaData = this.columnsMetaData ::: List(columnMetaData);
	}
	
	def getColumnMetaData(columnName:String) : Option[MorphColumnMetaData] = {
//	  val columnMetaData = this.columnsMetaData.find(p => p.columnName.equals(columnName));
//	  columnMetaData;
	  
	  val result = if(this.columnsMetaData == null) {None} else {
	    this.columnsMetaData.find(x => {
	    val xColumnName = MorphSQLUtility.printWithoutEnclosedCharacters(x.columnName, dbType);
	    val inputColumnName = MorphSQLUtility.printWithoutEnclosedCharacters(columnName, dbType);
	    xColumnName.equalsIgnoreCase(inputColumnName)
	 });	    
	  } 

	  result
	}
	
	def getTableRows() = this.tableRows;
}

object MorphTableMetaData {
	val logger = LoggerFactory.getLogger(this.getClass());
	
	def buildTablesMetaData(conn:Connection, databaseName:String, databaseType:String) 
	: List[MorphTableMetaData] = {
	  logger.debug("\tBuilding Tables MetaData for database: " + databaseName);
		
		val morphInformationSchema = MorphInformationSchema.apply(databaseType);
		val relNameColumn = morphInformationSchema.relNameColumn;
		val tableRowsColumn = morphInformationSchema.tableRowsColumn;
		val columnNameColumn = morphInformationSchema.columnNameColumn;
		val datatypeColumn = morphInformationSchema.datatypeColumn;
		val isNullableColumn = morphInformationSchema.isNullableColumn;
		val stmt = conn.createStatement();
		
		//GETTING SIZE OF THE TABLES
		val queryTablesMetadata:String = {
			if(databaseType.equalsIgnoreCase(Constants.DATABASE_MYSQL)) {
				"SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA = '" + databaseName + "'";
			} else if(databaseType.equalsIgnoreCase(Constants.DATABASE_POSTGRESQL)) {
				"SELECT * FROM pg_stat_user_tables ";
			} else {
			  null;
			}
		}

		var result:List[MorphTableMetaData] = Nil;
		if(queryTablesMetadata != null) {
			val rs = stmt.executeQuery(queryTablesMetadata);
			while(rs.next()) {
				val tableName = rs.getString(relNameColumn);
				val tableRows = rs.getLong(tableRowsColumn);
				val tableMetaData = new MorphTableMetaData(tableName, tableRows, null, databaseType);
				result = result ::: List(tableMetaData);
			}
		}

		result
	}
	
	def buildTableMetaData(tableName:String, dbMetaData:MorphDatabaseMetaData) 
	: MorphTableMetaData = {
	  logger.debug("\tBuilding Table MetaData for table: " + tableName);
    
	  val dbType = dbMetaData.dbType;
	  val dbName = dbMetaData.dbName;
	  val conn = dbMetaData.conn;
	  
		//TableMetaData tableMetaData = tablesMetaData.get(tableName);
	/*				if(tableMetaData == null) {
						logger.info("building table metadata for " + tableName);
	
						java.sql.Statement stmt = conn.createStatement();
						String query = "SELECT COUNT(*) FROM " + tableName + " T";
						ResultSet rs = stmt.executeQuery(query);
						rs.next();
						long tableRows = rs.getLong(1);
						tableMetaData = new TableMetaData(tableName, tableRows);
						tablesMetaData.put(tableName, tableMetaData);					
					}
	*/				
		val optionTableMetaData = dbMetaData.getTableMetaData(tableName);
		val tableMetaData = {
			if(optionTableMetaData.isDefined) {
				optionTableMetaData.get;
			} else {
				val stmt = conn.createStatement();
				val query = "SELECT COUNT(*) FROM " + tableName + " T";
				val rs = stmt.executeQuery(query);
				rs.next();
				val tableRows = rs.getLong(1);
				
				
				val tableMetaDataAux = new MorphTableMetaData(tableName, tableRows, null, dbType);
				dbMetaData.addTableMetaData(tableName, tableMetaDataAux);
				tableMetaDataAux
			}				  
		}
	
		tableMetaData;
	}
}