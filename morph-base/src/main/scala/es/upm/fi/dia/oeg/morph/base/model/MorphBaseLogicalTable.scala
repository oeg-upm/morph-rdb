package es.upm.fi.dia.oeg.morph.base.model

import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData

abstract class MorphBaseLogicalTable {
	var tableMetaData:Option[MorphTableMetaData] = None;

	def buildMetaData(dbMetaData:Option[MorphDatabaseMetaData]);
	
	def getLogicalTableSize() : Long = {
		if(this.tableMetaData.isDefined) { this.tableMetaData.get.getTableRows();} 
		else { -1 }
	}
	

}