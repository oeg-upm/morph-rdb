package es.upm.fi.dia.oeg.morph.base.model

import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData

abstract class MorphBaseLogicalTable {
	var tableMetaData:MorphTableMetaData = null;

	def buildMetaData(dbMetaData:MorphDatabaseMetaData);
	
	def getLogicalTableSize() : Long = {
		if(this.tableMetaData != null) { this.tableMetaData.getTableRows();	
		} else { -1 }
	}
	

}