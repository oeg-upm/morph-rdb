package es.upm.dia.fi.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType

class R2RMLTable(tableName:String )
extends R2RMLLogicalTable(LogicalTableType.TABLE_NAME) {
		if(tableName == null || tableName.equals("")) {
			throw new Exception("Empty table name specified!");
		}
	override def getValue() : String = { this.tableName; }
}