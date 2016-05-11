package es.upm.fi.dia.oeg.morph.base.sql

import es.upm.fi.dia.oeg.morph.base.Constants
import java.util.Random


trait SQLLogicalTable {
	var alias:String=null;
	var databaseType:String=null;
	
//	def generateAlias() : String = {
//			//return R2OConstants.VIEW_ALIAS + this.hashCode();
//			if(this.alias == null) {
//				this.alias = Constants.VIEW_ALIAS + new Random().nextInt(10000);
//			}
//			this.alias;
//	}
	def generateAlias():String;
	def setAlias(alias:String);
	def getAlias():String;
	
	def print(withAlias:Boolean ):String ;
	
	def setDatabaseType(databaseType:String) = {this.databaseType = databaseType};
	def getDatabaseType() : String = this.databaseType;

	def sameTableWith(anotherFromItem : SQLLogicalTable) = {
		val thisWithoutAlias = this.print(false)
		val anotherWithoutAlias = anotherFromItem.print(false);
		thisWithoutAlias.equals(anotherWithoutAlias)
	}
}