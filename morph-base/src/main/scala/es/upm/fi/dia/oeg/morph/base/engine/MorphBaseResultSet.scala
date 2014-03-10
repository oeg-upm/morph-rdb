package es.upm.fi.dia.oeg.morph.base.engine

abstract class MorphBaseResultSet {
	var columnNames:List[String]  = null;

	def next() : Boolean;
	def getString(columnIndex:Int) : String;
	def getString(columnLabel:String) : String;
	def getInt(columnIndex:Int ) : Integer;
	def getInt(columnLabel:String) : Integer;

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