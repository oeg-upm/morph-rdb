package es.upm.fi.dia.oeg.obdi.core.model;

import java.sql.Connection;

import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData;
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData;




public abstract class AbstractLogicalTable {
	//protected Map<String, ColumnMetaData> columnsMetaData;
	protected MorphTableMetaData tableMetaData;
	protected AbstractConceptMapping owner;
	
	public AbstractConceptMapping getOwner() {
		return owner;
	}

	public abstract void buildMetaData(MorphDatabaseMetaData dbMetaData) throws Exception;
	
	public MorphTableMetaData getTableMetaData() {
		return this.tableMetaData;
	}

	public long getLogicalTableSize() {
		long result = -1;
		if(this.tableMetaData != null) {
			result = this.tableMetaData.getTableRows();	
		}
		return result;
		
	}

//	public Map<String, ColumnMetaData> getColumnsMetaData() {
//		return columnsMetaData;
//	}
}
