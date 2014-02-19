package es.upm.dia.fi.oeg.morph.r2rml

import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTermMap
import Zql.ZExpression
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZConstant
import java.util.Arrays
import es.upm.fi.dia.oeg.morph.base.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import es.upm.fi.dia.oeg.morph.base.sql.SQLDataType
import scala.collection.JavaConversions._
import es.upm.dia.fi.oeg.morph.r2rml.model.MorphR2RMLTermMap

class MorphR2RMLUtility {

}

object MorphR2RMLUtility {
	val logger = Logger.getLogger("MorphR2RMLUtility");
		
	def generateCondForWellDefinedURI(termMap:MorphR2RMLTermMap, uri:String , alias:String 
			//, columnsMetaData:Map[String, ColumnMetaData] 
			//, tableMetaData:TableMetaData
			) : ZExpression = {
			val logicalTable = termMap.owner.getLogicalTable();
			val logicalTableMetaData = logicalTable.getTableMetaData();
			val conn = logicalTable.getOwner().getOwner().getConn();

			val tableMetaData = {
					if(logicalTableMetaData == null && conn != null) {
						try {
							logicalTable.buildMetaData(conn);
							logicalTable.getTableMetaData();
						} catch {
						case e:Exception => {
							logger.error(e.getMessage());
							throw new Exception(e.getMessage());
						}
						}
					} else {
						logicalTableMetaData
					}		  
			}		

			val result:ZExpression = {
					if(termMap.termMapType == Constants.MorphTermMapType.TemplateTermMap) {
						val matchedColValues = termMap.getTemplateValues(uri);
						if(matchedColValues == null || matchedColValues.size == 0) {
							val errorMessage = "uri " + uri + " doesn't match the template : " + termMap.templateString;
							logger.debug(errorMessage);
							null
						} else {
							val exprs:List[ZExpression] = {
								val exprsAux = matchedColValues.keySet.map(pkColumnString => {
									val value = matchedColValues(pkColumnString);

									val termMapColumnTypeName = termMap.columnTypeName;
									val columnTypeName = {
											if(termMapColumnTypeName != null) {
												termMapColumnTypeName
											} else {
												if(tableMetaData != null && tableMetaData.getColumnMetaData(pkColumnString).isDefined) {
													val columnTypeNameAux = tableMetaData.getColumnMetaData(pkColumnString).get.dataType;
													termMap.columnTypeName = columnTypeNameAux;
													columnTypeNameAux
												} else {
													null
												}
											}
									}

									val pkColumnConstant = MorphSQLConstant.apply(
											alias + "." + pkColumnString
											, ZConstant.COLUMNNAME, tableMetaData.dbType);

									val pkValueConstant = {
											if(columnTypeName != null) {
											  
												if(SQLDataType.isDatatypeNumber(columnTypeName)) {
													new ZConstant(value, ZConstant.NUMBER);
												} else if(SQLDataType.isDatatypeString(columnTypeName)) {
													new ZConstant(value, ZConstant.STRING);
												} else {
													new ZConstant(value, ZConstant.STRING);
												}					
											} else {
												new ZConstant(value, ZConstant.STRING);
											}					  
									}

									val expr = new ZExpression("=", pkColumnConstant, pkValueConstant);
									expr;				  
								})
								exprsAux.toList;
						}

						MorphSQLUtility.combineExpresions(
								exprs, Constants.SQL_LOGICAL_OPERATOR_AND);				
						}
					} else {
						null
					}
			}

			logger.debug("generateCondForWellDefinedURI = " + result);
			result;
	}  
}