package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import java.util.Arrays
import java.util.Collection
import java.util.Map
import java.util.Vector
import org.apache.log4j.Logger
import Zql.ZConstant
import Zql.ZExpression
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import es.upm.fi.dia.oeg.morph.base.ColumnMetaData
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphSQLUtility
import es.upm.fi.dia.oeg.obdi.core.exception.InsatisfiableSQLExpression
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractLogicalTable
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.obdi.core.sql.SQLDataType
import es.upm.fi.dia.oeg.obdi.core.sql.SQLUtility
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphAlphaResult
import es.upm.fi.dia.oeg.obdi.core.exception.QueryTranslationException
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseCondSQLGenerator
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslator
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.base.TableMetaData

class MorphRDBCondSQLGenerator(
    owner: IQueryTranslator
    ) 
extends MorphBaseCondSQLGenerator(
    owner: IQueryTranslator
    ) {
  	override val logger = Logger.getLogger("MorphCondSQLGenerator");

	override def genCondSQLPredicateObject(tp:Triple, alphaResult:MorphAlphaResult 
	    , betaGenerator:MorphBaseBetaGenerator, cm:AbstractConceptMapping , pm:AbstractPropertyMapping ) 
					: ZExpression = {
		val tpObject = tp.getObject();
		val logicalTableAlias = alphaResult.alphaSubject.getAlias();
		
		val poMap = pm.asInstanceOf[R2RMLPredicateObjectMap];

		val logicalTable = cm.getLogicalTable();
		val logicalTableMetaData = logicalTable.getTableMetaData();
		val conn = this.owner.getConnection();
		val tableMetaData = {
			if(logicalTableMetaData == null && conn != null) {
				try {
					logicalTable.buildMetaData(conn);
					logicalTable.getTableMetaData();
				} catch {
				  case e:Exception => {
				    logger.error(e.getMessage());
				    throw new QueryTranslationException(e.getMessage());
				  }
				}
			} else {
			  logicalTableMetaData
			}		  
		}
		
		val refObjectMap = poMap.getRefObjectMap();
		val objectMap = poMap.getObjectMap();
		if(refObjectMap == null && objectMap == null) {
			val errorMessage = "no mappings is specified.";
			logger.error(errorMessage);
			null
		} else if (refObjectMap != null && objectMap != null) {
			val errorMessage = "Wrong mapping, ObjectMap and RefObjectMap shouldn't be specified at the same time.";
			logger.error(errorMessage);			  
		}
		
		val  result2:ZExpression = {
			if(tpObject.isLiteral()) {
			  	if(refObjectMap == null && objectMap == null) {
			  		val errorMessage = "triple.object is a literal, but RefObjectMap is specified instead of ObjectMap";
			  		logger.error(errorMessage);
			  	} 
			  		
				val objectLiteralValue = tpObject.getLiteral().getValue();
	
				if(objectMap != null) {
					val columnName = objectMap.getColumnName();
					if(columnName != null) {
						val columnNameWithAlias = {
							if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
								logicalTableAlias + "." + columnName;
							} else {
							  columnName
							}					  
						}
	
						val columnConstant = new ZConstant(columnNameWithAlias,  ZConstant.COLUMNNAME);
						val objectLiteral = new ZConstant(objectLiteralValue.toString(), ZConstant.STRING);
						new ZExpression("=", columnConstant, objectLiteral);
					} else {
					  null
					}
				} else {
				  null
				}
			} else if(tpObject.isURI()) {
				if(refObjectMap == null && objectMap == null) {
					null
				} else if (refObjectMap != null && objectMap != null) {
					null			  
				} else if(objectMap != null && refObjectMap == null) {
					val uri = tpObject.getURI();
					val termMapType = objectMap.getTermMapType();
					if(termMapType == TermMapType.TEMPLATE) {
						this.generateCondForWellDefinedURI(objectMap
								, uri, logicalTableAlias, tableMetaData);
					} else if(termMapType == TermMapType.COLUMN) {
						val columnName = objectMap.getColumnName();
						val columnNameWithAlias = {
							if(logicalTableAlias != null) {
								logicalTableAlias + "." + columnName;
							} else {
							  columnName
							}					  
						}
	
						val zConstantObjectColumn = new ZConstant(columnNameWithAlias,  ZConstant.COLUMNNAME);
						val zConstantObjectURI = new ZConstant(uri.toString(), ZConstant.STRING);
						new ZExpression("=", zConstantObjectColumn, zConstantObjectURI);
					} else if(termMapType == TermMapType.CONSTANT) {
						//TODO
					  null
					} else {
					  null
					}
				} else if(refObjectMap != null && objectMap == null) {
					val refObjectMapAlias = this.owner.getTripleAlias(tp);
					
					//Collection<R2RMLJoinCondition> joinConditions = refObjectMap.getJoinConditions();
					//ZExp onExpression = R2RMLUtility.generateJoinCondition(joinConditions, logicalTableAlias, refObjectMapAlias);
					// onExpression done in alpha generator
	
					val parentTriplesMap = 
							refObjectMap.getParentTriplesMap();
					val uriCondition = this.generateCondForWellDefinedURI(
							parentTriplesMap.getSubjectMap(), tpObject.getURI(),
							refObjectMapAlias, tableMetaData);
	
					val expressionsList = List(uriCondition);
					MorphSQLUtility.combineExpresions(expressionsList, Constants.SQL_LOGICAL_OPERATOR_AND);				
				} else {
				  null
				}
			} else if(tpObject.isVariable()) {
				null
			} else {
			  null
			}
		}
		
		result2;
	}

	def generateCondForWellDefinedURI(termMap:R2RMLTermMap, uri:String , alias:String 
	    //, columnsMetaData:Map[String, ColumnMetaData] 
	    , tableMetaData:TableMetaData
	) : ZExpression = {
		
		
		val result:ZExpression = {
			if(termMap.getTermMapType() == TermMapType.TEMPLATE) {
				val matchedColValues = termMap.getTemplateValues(uri);
				if(matchedColValues == null || matchedColValues.size() == 0) {
					val errorMessage = "uri " + uri + " doesn't match the template : " + termMap.getTemplateString();
					logger.debug(errorMessage);
					null
				} else {
					val exprs:List[ZExpression] = {
						val exprsAux = matchedColValues.keySet().map(pkColumnString => {
							val value = matchedColValues.get(pkColumnString);
							
							
							val termMapColumnTypeName = termMap.getColumnTypeName();
							val columnTypeName = {
							  if(termMapColumnTypeName != null) {
							    termMapColumnTypeName
							  } else {
							    if(tableMetaData != null && tableMetaData.getColumnMetaData(pkColumnString).isDefined) {
									val columnTypeNameAux = tableMetaData.getColumnMetaData(pkColumnString).get.dataType;
									termMap.setColumnTypeName(columnTypeNameAux);
									columnTypeNameAux
							    } else {
							      null
							    }
							  }
							}

		
		
							val pkColumnConstant = MorphSQLConstant.apply(
									alias + "." + pkColumnString, ZConstant.COLUMNNAME, databaseType);
							
							val pkValueConstant = {
								if(columnTypeName != null) {
									if(Arrays.asList(SQLDataType.datatypeNumber).contains(columnTypeName)) {
										new ZConstant(value, ZConstant.NUMBER);
									} else if(Arrays.asList(SQLDataType.datatypeString).contains(columnTypeName)) {
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

	override def genCondSQLSubjectURI(tpSubject:Node , alphaResult:MorphAlphaResult 
	    , cm:AbstractConceptMapping ) : ZExpression = {
		val subjectURI = tpSubject.getURI();
		val tm = cm.asInstanceOf[R2RMLTriplesMap];
		val subjectURIConstant = new ZConstant(subjectURI, ZConstant.STRING);
		val logicalTableAlias = alphaResult.alphaSubject.getAlias();
		val subjectTermMapType = tm.getSubjectMap().getTermMapType();
		
		val result2:ZExpression = {
			if(subjectTermMapType == TermMapType.TEMPLATE) {
				try {
					val logicalTable = cm.asInstanceOf[R2RMLTriplesMap].getLogicalTable();
					val logicalTableMetaData = logicalTable.getTableMetaData();
					//val logicalTableColumnsMetaData = logicalTable.getColumnsMetaData();;
					val tableMetaData = {
						if(logicalTableMetaData == null) {
							val conn = this.owner.getConnection();
							logicalTable.buildMetaData(conn);
							logicalTable.getTableMetaData();
						} else {
						  logicalTableMetaData
						}				  
					}
	
					this.generateCondForWellDefinedURI(tm.getSubjectMap(), 
							tpSubject.getURI(), logicalTableAlias, tableMetaData);					
				} catch {
				  case e:Exception => {
					logger.error(e.getMessage());
				    throw new QueryTranslationException(e);
				  }
				}
			} else if(subjectTermMapType == TermMapType.COLUMN){
				val subjectMapColumn = new ZConstant(tm.getSubjectMap().getColumnName(), ZConstant.COLUMNNAME);
				new ZExpression("=", subjectMapColumn, subjectURIConstant);
			} else { //subjectTermMapType == TermMapType.CONSTANT
				val subjectMapColumn = new ZConstant(tm.getSubjectMap().getConstantValue(), ZConstant.COLUMNNAME);
				new ZExpression("=", subjectMapColumn, subjectURIConstant);
			}
		}

		result2;
	}

}