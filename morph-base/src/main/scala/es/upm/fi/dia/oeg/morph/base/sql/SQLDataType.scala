package es.upm.fi.dia.oeg.morph.base.sql

class SQLDataType {

}

object SQLDataType {
	val datatypeNumber = List("INT", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE", "FLOAT", "DECIMAL ", "NUMERIC", "int4");
	val datatypeString = List("CHAR", "VARCHAR", "LONGVARCHAR");
  
	def isDatatypeNumber(pDatatype:String ) = {
	  val result = this.datatypeNumber.exists(p => p.equalsIgnoreCase(pDatatype));
	  result
	}
	
	def isDatatypeString(pDatatype:String ) = {
	  val result = this.datatypeString.exists(p => p.equalsIgnoreCase(pDatatype));
	  result
	}
	
}