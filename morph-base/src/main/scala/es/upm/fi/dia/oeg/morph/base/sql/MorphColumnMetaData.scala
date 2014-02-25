package es.upm.fi.dia.oeg.morph.base.sql

import org.apache.log4j.Logger
import java.sql.Connection
import scala.collection.JavaConversions._

class MorphColumnMetaData (val tableName : String, val columnName : String
    , val dataType : String, val isNullable : Boolean, val characterMaximumLength:Integer) {

	val logger = Logger.getLogger("ColumnMetaData");

//	override def toString() = {
//		val result = "ColumnMetaData [tableName=" + tableName + ", columnName=" + columnName + ", dataType=" + dataType + ", isNullable=" + isNullable + "]";
//		result;
//	}
	
//	def getDataType() = {
//	  this.dataType;
//	}
}