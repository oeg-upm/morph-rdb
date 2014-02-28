package es.upm.dia.fi.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType

class R2RMLSQLQuery(sqlQuery:String ) 
extends R2RMLLogicalTable(LogicalTableType.QUERY_STRING) {

	override def getValue() : String = { this.sqlQuery; }
}