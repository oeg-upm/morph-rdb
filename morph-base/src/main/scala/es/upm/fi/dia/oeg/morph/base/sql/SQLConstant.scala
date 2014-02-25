package es.upm.fi.dia.oeg.morph.base.sql

import Zql.ZConstant

class SQLConstant(arg0: String, arg1: Int) extends ZConstant(arg0, arg1) {
	var columnType : String = null;
	
	def this(zConstant : ZConstant) {
	  this(zConstant.getValue(), zConstant.getType());
	}
	
	override def toString() : String = {
	  var result : String = null;
	  
	  if(this.columnType == null) {
	    result = super.toString;
	  } else {
	    result = super.toString + "::" + this.columnType;
	  }
	  
	  result;
	}
	
	def setColumnType(columnType : String) = {
	  this.columnType = columnType;
	}
	
}