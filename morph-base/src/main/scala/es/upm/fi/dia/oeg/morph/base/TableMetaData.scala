package es.upm.fi.dia.oeg.morph.base

import org.apache.log4j.Logger
import java.sql.Connection
import scala.collection.JavaConversions._


class TableMetaData(val tableName:String , val tableRows:Long ) {
	val logger = Logger.getLogger("TableMetaData");
}

/*object TableMetaData {
 
}*/