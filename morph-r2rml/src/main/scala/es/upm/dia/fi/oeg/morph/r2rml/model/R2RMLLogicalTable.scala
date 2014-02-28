package es.upm.dia.fi.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.model.MorphBaseLogicalTable
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.dia.fi.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType
import es.upm.dia.fi.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility

abstract class R2RMLLogicalTable(val logicalTableType:LogicalTableType ) 
extends MorphBaseLogicalTable with MorphR2RMLElement{
	val logger = Logger.getLogger(this.getClass().getName());
	var alias:String = null;

	def buildMetaData(dbMetaData:MorphDatabaseMetaData ) = {
		val tableName = this match {
		  case r2rmlTable:R2RMLTable => {
		    val dbType = dbMetaData.dbType;
		    val enclosedChar = Constants.getEnclosedCharacter(dbType);
		    val tableNameAux = r2rmlTable.getValue().replaceAll("\"", enclosedChar);
		    tableNameAux
		  }
		  case r2rmlSQLQuery:R2RMLSQLQuery => {
			val queryStringAux = r2rmlSQLQuery.getValue().trim();
			val queryString = {
				if(queryStringAux.endsWith(";")) {
					queryStringAux.substring(0, queryStringAux.length()-1);
				} else {queryStringAux}
			}
			"(" + queryString + ")";		    
		  }
		}
		
		val optionTableMetaData = dbMetaData.getTableMetaData(tableName);
		val tableMetaData = {
			if(optionTableMetaData.isDefined) {
				optionTableMetaData.get;
			} else {
				MorphTableMetaData.buildTableMetaData(tableName, dbMetaData);
			}		  
		}

		this.tableMetaData = tableMetaData;
	}

	def getValue() : String;

	override def toString() : String = {
		val result = this match {
		  case _:R2RMLTable => { "R2RMLTable"; }
		  case _:R2RMLSQLQuery => { "R2RMLSQLQuery"; } 
		  case _ => { "" }		  
		}
		  
		result + ":" + this.getValue();
	}


	def accept(visitor:MorphR2RMLElementVisitor ) : Object  = {
		val result = visitor.visit(this);
		result;
	}
}

object R2RMLLogicalTable {
	val logger = Logger.getLogger(this.getClass().getName());

	def parse(resource:Resource ) : R2RMLLogicalTable  = {
		 
		val tableNameStatement = resource.getProperty(Constants.R2RML_TABLENAME_PROPERTY);
		val logicalTable : R2RMLLogicalTable = if(tableNameStatement != null) {
			val tableName = tableNameStatement.getObject().toString();
			new R2RMLTable(tableName);			
		} else {
			val sqlQueryStatement = resource.getProperty(
					Constants.R2RML_SQLQUERY_PROPERTY);
			if(sqlQueryStatement == null) {
				logger.error("Invalid logical table defined : " + resource);
			}
			val sqlQueryStringAux = sqlQueryStatement.getObject().toString().trim();
			val sqlQueryString = if(sqlQueryStringAux.endsWith(";")) {
				sqlQueryStringAux.substring(0, sqlQueryStringAux.length()-1);
			} else {
			  sqlQueryStringAux
			}
			
			new R2RMLSQLQuery(sqlQueryString);
		}

		logicalTable;
	}  
}