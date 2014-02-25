package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType;


public class R2RMLSQLQuery extends R2RMLLogicalTable {
	private String sqlQuery;
	
	public R2RMLSQLQuery(String sqlQuery) {
		this.sqlQuery = sqlQuery;
		super.logicalTableType = LogicalTableType.QUERY_STRING;
	}

	@Override
	public String getValue() {
		return this.sqlQuery;
	}
}
