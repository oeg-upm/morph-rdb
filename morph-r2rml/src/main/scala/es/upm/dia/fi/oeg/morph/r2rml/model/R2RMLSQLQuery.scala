package es.upm.dia.fi.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants


class R2RMLSQLQuery(sqlQuery:String ) 
extends R2RMLLogicalTable(Constants.LogicalTableType.QUERY_STRING) {

	override def getValue() : String = { this.sqlQuery; }
}