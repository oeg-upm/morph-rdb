package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.Constants

abstract class MorphBaseDataSourceReader {
	def execute(query:String): MorphBaseResultSet;
	//def execute(query:String): Boolean;
	def setConnection(obj:Object);
	def setTimeout(timeout:Int);
	def closeConnection();
}

object MorphBaseDataSourceReader {
  	def apply(dataSourceReaderClassName:String, connection:Connection
  	    , timeout:Int) : MorphBaseDataSourceReader ={
		val className = if(dataSourceReaderClassName == null || dataSourceReaderClassName.equals("")) {
			Constants.DATASOURCE_READER_CLASSNAME_DEFAULT;
		} else {
			dataSourceReaderClassName;
		}
		
		val classInstance = Class.forName(className).newInstance()
		val dataSourceReader = classInstance.asInstanceOf[MorphBaseDataSourceReader];
		dataSourceReader.setConnection(connection);
		dataSourceReader.setTimeout(timeout)
				
		dataSourceReader
	}	
}