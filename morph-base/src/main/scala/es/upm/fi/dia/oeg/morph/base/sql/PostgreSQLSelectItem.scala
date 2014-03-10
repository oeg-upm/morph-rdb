package es.upm.fi.dia.oeg.morph.base.sql

import Zql.ZSelectItem

class PostgreSQLSelectItem extends ZSelectItem {
	var datatype:String = null;
	
	def setDatatype(datatype:String ) = {
		this.datatype = datatype;
	}

	override def toString() :String  = {
		
		val result = if(this.datatype == null) {
			super.toString();
		} else {
			val expString = this.getExpression().toString() + "::" + this.datatype;
			
			val alias = this.getAlias();  
			if(alias != null && !alias.equals("")) {
				expString + " AS " + alias;
			} else {
			  expString;
			}
		}
		result;
	}

}