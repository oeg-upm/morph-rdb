package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable


class R2RMLTable(tableName:String )
extends R2RMLLogicalTable(Constants.LogicalTableType.TABLE_NAME) {
		if(tableName == null || tableName.equals("")) {
			throw new Exception("Empty table name specified!");
		}
	override def getValue() : String = { this.tableName; }
}