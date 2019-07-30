package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.ResultSet

abstract class MorphBaseResultSet {
	var columnNames:List[String]  = null;

	def next() : Boolean;
	def getString(columnIndex:Int) : String;
	def getString(columnLabel:String) : String;
	def getInt(columnIndex:Int ) : Int;
	def getInt(columnLabel:String) : Int;
	def getObject(columnIndex:Int ) : java.lang.Object;
	def getObject(columnLabel:String) : java.lang.Object;

	def getColumnNames() : List[String]  = {
		this.columnNames;
	}

	def setColumnNames(columnNames:List[String] ) {
		this.columnNames = columnNames;
	}

	def getColumnName(columnIndex:Int ) = {
		this.columnNames(columnIndex);
	}
}