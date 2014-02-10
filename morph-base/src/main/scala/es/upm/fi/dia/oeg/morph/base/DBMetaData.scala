package es.upm.fi.dia.oeg.morph.base

import org.apache.log4j.Logger
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.sql.MorphInformationSchema

class DBMetaData(val dbName:String, val dbType:String, var tablesMetaData : List[TableMetaData]) {
	def this(dbName:String, dbType:String) = {
	  this(dbName, dbType, Nil)
	}

	def getTableMetaData(tableName:String) : Option[TableMetaData] = {
	  val result = this.tablesMetaData.find(x => {
	    val xTableName = MorphSQLUtility.printWithoutEnclosedCharacters(x.tableName, dbType);
	    val inputTableName = MorphSQLUtility.printWithoutEnclosedCharacters(tableName, dbType);
	    xTableName.equalsIgnoreCase(inputTableName)
	 });
	  
	  result
	}
	
	def addTableMetaData(tableName:String, tableMetaData:TableMetaData) = {
	  this.tablesMetaData = this.tablesMetaData ::: List(tableMetaData);
	}
}

object DBMetaData {
	val logger = Logger.getLogger("TableMetaData");
	
 	def buildDBMetaData(conn:Connection , dbName:String , dbType:String ) 
 	: DBMetaData = {
		val dbMetaData : DBMetaData = {
	 		if(conn != null) {
				try {
					var listTableMetaData = TableMetaData.buildTablesMetaData(conn, dbName, dbType);
					val mapColumnsMetaData = ColumnMetaDataFactory.buildMapColumnsMetaData(conn, dbName, dbType);
					val commonTableNames = listTableMetaData.map(x => x.tableName).toSet.intersect(mapColumnsMetaData.keySet);
					for(tableName <- commonTableNames) {
						val tableMetaData = listTableMetaData.find(p => p.tableName.equals(tableName)).get;
						val columnsMetaData = mapColumnsMetaData(tableName);
						tableMetaData.columnsMetaData = columnsMetaData;
					}
					new DBMetaData(dbName, dbType, listTableMetaData);
				} catch {
				  case e:Exception => {
				    logger.error("Error while getting table meta data");
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