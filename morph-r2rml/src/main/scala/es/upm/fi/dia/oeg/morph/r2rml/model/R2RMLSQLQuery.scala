package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable


class R2RMLSQLQuery(sqlQuery:String ) 
extends R2RMLLogicalTable(Constants.LogicalTableType.QUERY_STRING) {

	override def getValue() : String = { this.sqlQuery; }
}