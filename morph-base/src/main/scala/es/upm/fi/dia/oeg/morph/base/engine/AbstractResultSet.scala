package es.upm.fi.dia.oeg.morph.base.engine

abstract class AbstractResultSet {
	var columnNames:java.util.LinkedList[String]  = null;

	def next() : Boolean;
	def getString(columnIndex:Int) : String;
	def getString(columnLabel:String) : String;
	def getInt(columnIndex:Int ) : Integer;
	def getInt(columnLabel:String) : Integer;

	def getColumnNames() : java.util.LinkedList[String]  = {
		this.columnNames;
	}

	def setColumnNames(columnNames:java.util.LinkedList[String] ) {
		this.columnNames = columnNames;
	}
	
	def getColumnName(columnIndex:Int ) = {
		this.columnNames.get(columnIndex);
	}
}