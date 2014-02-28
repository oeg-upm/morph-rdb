package es.upm.dia.fi.oeg.morph.r2rml.model

trait IColumnTermMap {
	var columnName:String =null;
//	var columnTypeName:String=null;
	
	def getColumnName() : String = { this.columnName};
//	def getColumnTypeName():String = {this.columnTypeName;}
//	def setColumnTypeName(columnTypeName:String)= {this.columnTypeName=columnTypeName;}
}