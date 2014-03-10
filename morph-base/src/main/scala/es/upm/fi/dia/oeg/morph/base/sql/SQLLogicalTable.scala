package es.upm.fi.dia.oeg.morph.base.sql

trait SQLLogicalTable {
//	var alias:String=null;
	var databaseType:String=null;
	
	def generateAlias():String;
	def setAlias(alias:String);
	def getAlias():String;
	
	def print(withAlias:Boolean ):String ;
	
	def setDatabaseType(databaseType:String) = {this.databaseType = databaseType};
	def getDatabaseType() : String = this.databaseType;
}