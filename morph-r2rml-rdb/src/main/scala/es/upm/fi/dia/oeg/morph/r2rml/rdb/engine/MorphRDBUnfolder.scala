package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import java.util.Collection
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.base.Constants
import java.util.HashSet
import Zql.ZQuery
import Zql.ZSelectItem
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLPredicateMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLLogicalTable
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTable
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLSQLQuery
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLJoinCondition
import Zql.ZExpression
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.dia.fi.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLFromItem
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.sql.IQuery

class MorphRDBUnfolder(md:R2RMLMappingDocument) 
extends MorphBaseUnfolder(md) with MorphR2RMLElementVisitor {
	var mapTermMapColumnsAliases:Map[Object, List[String]] = Map.empty;
	val logger = Logger.getLogger(this.getClass().getName());
	var mapRefObjectMapAlias:Map[R2RMLRefObjectMap, String] = Map.empty;
	

	def getAliases(termMapOrRefObjectMap:Object ) : Collection[String] = {
	  if(this.mapTermMapColumnsAliases.get(termMapOrRefObjectMap).isDefined) {
	    this.mapTermMapColumnsAliases(termMapOrRefObjectMap);
	  } else {
	    null
	  }
	}

	def getMapRefObjectMapAlias() : Map[R2RMLRefObjectMap, String] = {
		return mapRefObjectMapAlias;
	}

	def unfoldLogicalTable(logicalTable:R2RMLLogicalTable) : SQLLogicalTable = {
		//val dbType = md.configurationProperties.databaseType;
//		val dbType = if(md.dbMetaData.isDefined) { md.dbMetaData.get.dbType; }
//		else { Constants.DATABASE_DEFAULT }
		  
		val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
			  
		val logicalTableType = logicalTable.logicalTableType;
		val result = logicalTableType match {
		  case Constants.LogicalTableType.TABLE_NAME => {
			  val logicalTableValue = logicalTable.getValue();
			  val logicalTableValueWithEnclosedChar = logicalTableValue.replaceAll("\"", dbEnclosedCharacter);
			  val resultAux = new SQLFromItem(logicalTableValueWithEnclosedChar
			      , Constants.LogicalTableType.TABLE_NAME);
			  resultAux.databaseType = this.dbType;
			  resultAux
			} 
		  case Constants.LogicalTableType.QUERY_STRING => {
				val sqlString = logicalTable.getValue().replaceAll("\"", dbEnclosedCharacter);
				try {
					val sqlString2 = if(!sqlString.endsWith(";")) {
						sqlString + ";";
					} else {
					  sqlString
					}
					MorphRDBUtility.toSQLQuery(sqlString2);
				} 
				catch {
				  case e:Exception => {
					logger.warn("Not able to parse the query, string will be used.");
					val resultAux = new SQLFromItem(sqlString, Constants.LogicalTableType.QUERY_STRING);
					resultAux.databaseType = this.dbType;
					resultAux
				  }
				}
			} 
		  case _ => {
				logger.warn("Invalid logical table type");
				null;
			}
		}

		result;
	}

	def unfoldTermMap(termMap:R2RMLTermMap, logicalTableAlias:String) 
	: List[MorphSQLSelectItem] =  {

		val result = if(termMap != null) {
			termMap.termMapType match {
			  case Constants.MorphTermMapType.TemplateTermMap => {
				val termMapReferencedColumns = termMap.getReferencedColumns();
				if(termMapReferencedColumns != null) {
					termMapReferencedColumns.map(termMapReferencedColumn => {
						val selectItem = MorphSQLSelectItem.apply(
						    termMapReferencedColumn, logicalTableAlias, dbType);
						if(selectItem != null) {
							if(selectItem.getAlias() == null) {
								val alias = selectItem.getTable() + "_" + selectItem.getColumn();
								selectItem.setAlias(alias);
								if(this.mapTermMapColumnsAliases.containsKey(termMap)) {
									val oldColumnAliases = this.mapTermMapColumnsAliases(termMap);
									val newColumnAliases = oldColumnAliases ::: List(alias);
									this.mapTermMapColumnsAliases += (termMap -> newColumnAliases);
								} else {
									this.mapTermMapColumnsAliases += (termMap -> List(alias));
								}								
							}
						}
						selectItem
					});
				} else { Nil }			    
			  }
			  case Constants.MorphTermMapType.ColumnTermMap => {
			    val termColumnName = termMap.columnName;
			    val selectItem = MorphSQLSelectItem.apply(
						    termColumnName, logicalTableAlias, dbType);
						
			    if(selectItem != null) {
			    	if(selectItem.getAlias() == null) {
			    		val alias = selectItem.getTable() + "_" + selectItem.getColumn();
			    		selectItem.setAlias(alias);
						if(this.mapTermMapColumnsAliases.containsKey(termMap)) {
							val oldColumnAliases = this.mapTermMapColumnsAliases(termMap);
							val newColumnAliases = oldColumnAliases ::: List(alias);
							this.mapTermMapColumnsAliases += (termMap -> newColumnAliases);
						} else {
							this.mapTermMapColumnsAliases += (termMap -> List(alias));
						}								
			    	}
			    }
			    List(selectItem)
			  }
			  case Constants.MorphTermMapType.ConstantTermMap => {
				  Nil;
			  }
			  case _ => {
			    throw new Exception("Invalid term map type!");
			  }
			}

	  } else {
	    Nil
	  }

	  result
	}
	

	
	def  unfoldTriplesMap(logicalTable:R2RMLLogicalTable, subjectMap:R2RMLSubjectMap
			, poms:Collection[R2RMLPredicateObjectMap] ) : IQuery = {
//		val triplesMap = subjectMap.getOwner();
//		logger.info("unfolding triplesMap : " + triplesMap);

		//unfold subjectMap
		//R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
		//val result = this.unfoldSubjectMap(subjectMap, logicalTable);
		
		val result = new SQLQuery();
		result.setDatabaseType(this.dbType);
		
		//UNFOLD LOGICAL TABLE
		val logicalTableUnfolded :SQLFromItem = logicalTable match {
		  case _:R2RMLTable => {
				this.unfoldLogicalTable(logicalTable).asInstanceOf[SQLFromItem];
			} 
		  case _:R2RMLSQLQuery => {
				val logicalTableAux = this.unfoldLogicalTable(logicalTable)
				logicalTableAux match {
				  case _:SQLQuery => {
						val zQuery = this.unfoldLogicalTable(logicalTable).asInstanceOf[ZQuery];
						val resultAux = new SQLFromItem(zQuery.toString(), Constants.LogicalTableType.QUERY_STRING);
						resultAux.databaseType = this.dbType
						resultAux
					} 
				  case sqlFromItem:SQLFromItem => { sqlFromItem; }
				  case _ => { null}
				}
			}
		  case _ => {null}
		}
		val logicalTableAlias = logicalTableUnfolded.generateAlias();
		logicalTable.alias = logicalTableAlias;
		//result.addFrom(logicalTableUnfolded);
		val logicalTableUnfoldedJoinTable = new SQLJoinTable(logicalTableUnfolded, null, null); 
		result.addFromItem(logicalTableUnfoldedJoinTable);

		val subjectMapSelectItems = this.unfoldTermMap(subjectMap, logicalTableAlias);
		result.addSelectItems(subjectMapSelectItems);
		
		
		//val logicalTableAlias = logicalTable.getAlias();
		if(poms != null) {
			for(predicateObjectMap <- poms) {
				//UNFOLD PREDICATEMAP
				val predicateMaps = predicateObjectMap.predicateMaps;
				if(predicateMaps != null && !predicateMaps.isEmpty()) {
					val predicateMap = predicateObjectMap.getPredicateMap(0);
					val predicateMapSelectItems = this.unfoldTermMap(predicateMap, logicalTableAlias);
					result.addSelectItems(predicateMapSelectItems);
				}


				//UNFOLD OBJECTMAP
				val objectMaps = predicateObjectMap.objectMaps;
				if(objectMaps != null && !objectMaps.isEmpty()) {
					val objectMap = predicateObjectMap.getObjectMap(0);
					val objectMapSelectItems = this.unfoldTermMap(objectMap, logicalTableAlias);
					result.addSelectItems(objectMapSelectItems);
				}


				//UNFOLD REFOBJECTMAP
				val refObjectMaps = predicateObjectMap.refObjectMaps;
				if(refObjectMaps != null && !refObjectMaps.isEmpty()) {
					val refObjectMap = predicateObjectMap.getRefObjectMap(0);
					if(refObjectMap != null) {
						val parentTriplesMap = this.md.getParentTriplesMap(refObjectMap);
						val parentLogicalTable = parentTriplesMap.getLogicalTable();
						if(parentLogicalTable == null) {
							val errorMessage = "Parent logical table is not found for RefObjectMap : " + predicateObjectMap.getMappedPredicateName(0);
							throw new Exception(errorMessage);
						}
						val sqlParentLogicalTable = this.unfoldLogicalTable(parentLogicalTable.asInstanceOf[R2RMLLogicalTable]);
						//parentLogicalTable.alias = parentLogicalTableAlias;
								
						val joinQueryAlias = sqlParentLogicalTable.generateAlias();
						sqlParentLogicalTable.setAlias(joinQueryAlias);
						//refObjectMap.setAlias(joinQueryAlias);
						this.mapRefObjectMapAlias += (refObjectMap -> joinQueryAlias);
						predicateObjectMap.setAlias(joinQueryAlias);
						
						//val refObjectMapColumnsString = refObjectMap.getParentDatabaseColumnsString();
						val parentSubjectMap = parentTriplesMap.subjectMap;
						val refObjectMapColumnsString = parentSubjectMap.getReferencedColumns;
						
						if(refObjectMapColumnsString != null ) {
							for(refObjectMapColumnString <- refObjectMapColumnsString) {
								val selectItem = MorphSQLSelectItem(
								    refObjectMapColumnString, joinQueryAlias, dbType, null);
								if(selectItem.getAlias() == null) {
									val alias = selectItem.getTable() + "_" + selectItem.getColumn();
									selectItem.setAlias(alias);
									if(this.mapTermMapColumnsAliases.containsKey(refObjectMap)) {
											val oldColumnAliases = this.mapTermMapColumnsAliases(refObjectMap);
											val newColumnAliases = oldColumnAliases ::: List(alias);
											this.mapTermMapColumnsAliases += (refObjectMap -> newColumnAliases);
									} else {
										this.mapTermMapColumnsAliases +=(refObjectMap -> List(alias));
									}
								}							
								//resultSelectItems.add(selectItem);
								result.addSelectItem(selectItem);
							}
						}


						
						val joinConditions = refObjectMap.getJoinConditions();
						val onExpression = MorphRDBUnfolder.unfoldJoinConditions(
						    joinConditions, logicalTableAlias, joinQueryAlias, dbType);
						val joinQuery = new SQLJoinTable(sqlParentLogicalTable
						    , Constants.JOINS_TYPE_LEFT, onExpression);
						//result.addJoinQuery(joinQuery);		
						result.addFromItem(joinQuery);
					}					
				}


			}
		}

//		if(resultSelectItems != null) {
//			for(ZSelectItem selectItem : resultSelectItems) {
//				result.addSelect(selectItem);
//			}
//		}
		//logger.info(triplesMap + " unfolded = \n" + result);

		result;		
	}


	
	def unfoldTriplesMap(triplesMap:R2RMLTriplesMap , subjectURI:String ) : IQuery  = {
		val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
		val subjectMap = triplesMap.subjectMap;
		val predicateObjectMaps = triplesMap.predicateObjectMaps;
		
		val resultAux = this.unfoldTriplesMap(logicalTable, subjectMap, predicateObjectMaps);
		val result = if(subjectURI != null) {
			val whereExpression = MorphRDBUtility.generateCondForWellDefinedURI(
			    subjectMap, triplesMap, subjectURI, logicalTable.alias);
			if(whereExpression != null) {
				resultAux.addWhere(whereExpression);
				resultAux;
			} else {
				null;
			} 
		} else {
			  resultAux;
			}
		result;		
	}
	
	def unfoldTriplesMap(triplesMap:R2RMLTriplesMap ) : IQuery  = {
		this.unfoldTriplesMap(triplesMap, null);
	}

	override def unfoldConceptMapping(cm:MorphBaseClassMapping) : IQuery  = {
		this.unfoldTriplesMap(cm.asInstanceOf[R2RMLTriplesMap]);
	}
	
	override def unfoldConceptMapping(cm:MorphBaseClassMapping,subjectURI:String):IQuery={
		this.unfoldTriplesMap(cm.asInstanceOf[R2RMLTriplesMap], subjectURI);
	}

	override def unfoldMappingDocument() = {
		val triplesMaps = this.md.classMappings
		val result = if(triplesMaps != null) {
			triplesMaps.flatMap(triplesMap => {
				try {
					val triplesMapUnfolded = this.unfoldConceptMapping(triplesMap);
					Some(triplesMapUnfolded);
				} catch {
				  case e:Exception => {
					logger.error("error while unfolding triplesMap : " + triplesMap);
					logger.error("error message = " + e.getMessage());
					None
				  }
				}			  
			})
		} else {
		  Nil
		}
		result;
	}

	override def unfoldSubject(cm:MorphBaseClassMapping) : IQuery = {
		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
		val subjectMap = triplesMap.subjectMap;
		val predicateObjectMaps = triplesMap.predicateObjectMaps;
		val result = this.unfoldTriplesMap(logicalTable, subjectMap, null);
		return result;
	}

//	def unfoldSubjectMap2(subjectMap:R2RMLSubjectMap, logicalTable:R2RMLLogicalTable):SQLQuery={
////		R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
////		R2RMLSubjectMap subjectMap = triplesMap.getSubjectMap();
//		
//		val result = new SQLQuery();
//		 
//
//		val logicalTableUnfolded :SQLFromItem = logicalTable match {
//		  case _:R2RMLTable => {
//				this.unfoldLogicalTable(logicalTable).asInstanceOf[SQLFromItem];
//			} 
//		  case _:R2RMLSQLQuery => {
//				val logicalTableAux = logicalTable.accept(this);
//				logicalTableAux match {
//				  case _:SQLQuery => {
//						val zQuery = this.unfoldLogicalTable(logicalTable).asInstanceOf[ZQuery];
//						new SQLFromItem(zQuery.toString(), LogicalTableType.QUERY_STRING, this.dbType);
//					} 
//				  case sqlFromItem:SQLFromItem => { sqlFromItem; }
//				  case _ => { null}
//				}
//			}
//		  case _ => {null}
//		}
//		
//		val logicalTableAlias = logicalTableUnfolded.generateAlias();
//		logicalTable.setAlias(logicalTableAlias);
//		//result.addFrom(logicalTableUnfolded);
//		val logicalTableUnfoldedJoinTable = new SQLJoinTable(logicalTableUnfolded, null, null); 
//		result.addFromItem(logicalTableUnfoldedJoinTable);
//
//		val subjectMapSelectItems = this.unfoldTermMap(subjectMap, logicalTableAlias).toSet;
//		val resultSelectItems : Set[ZSelectItem] = subjectMapSelectItems.toSet;
//		result.setSelectItems(resultSelectItems);
//		result;
//	}

	def visit(logicalTable:R2RMLLogicalTable ) : SQLLogicalTable  ={
		val result = this.unfoldLogicalTable(logicalTable);
		result;
	}

	def visit( md:R2RMLMappingDocument) : Collection[IQuery] = {
		val  result = this.unfoldMappingDocument();
		result;
	}

	def  visit( objectMap:R2RMLObjectMap) : Object = {
		// TODO Auto-generated method stub
		null;
	}

	def visit(refObjectMap:R2RMLRefObjectMap ) : Object = {
		// TODO Auto-generated method stub
		null;
	}

	def  visit(r2rmlTermMap:R2RMLTermMap ) : Object = {
		// TODO Auto-generated method stub
		null;
	}

	def visit(triplesMap:R2RMLTriplesMap ) : IQuery  = {
		val result = this.unfoldTriplesMap(triplesMap);
		result;
	}
}

object MorphRDBUnfolder {
	def unfoldJoinConditions(pJoinConditions:Iterable[R2RMLJoinCondition] 
	, parentTableAlias:String, joinQueryAlias:String , dbType:String ) : ZExpression  = {
		val joinConditions = {
		  if(pJoinConditions == null) { Nil }
		  else {pJoinConditions}
		}
		
		//var onExpression : ZExpression = null;
		val enclosedCharacter = Constants.getEnclosedCharacter(dbType);
		
		val joinConditionExpressions = joinConditions.map(joinCondition => {
			var childColumnName = joinCondition.childColumnName
			childColumnName = childColumnName.replaceAll("\"", enclosedCharacter);
			childColumnName = parentTableAlias + "." + childColumnName;
			val childColumn = new ZConstant(childColumnName, ZConstant.COLUMNNAME);

			var parentColumnName = joinCondition.parentColumnName;
			parentColumnName = parentColumnName.replaceAll("\"", enclosedCharacter);
			parentColumnName = joinQueryAlias + "." + parentColumnName;
			val parentColumn = new ZConstant(parentColumnName, ZConstant.COLUMNNAME);
				
			new ZExpression("=", childColumn, parentColumn);
		})
		
		val result = if(joinConditionExpressions.size > 0) {
		  MorphSQLUtility.combineExpresions(joinConditionExpressions, Constants.SQL_LOGICAL_OPERATOR_AND);
		} else {
		  Constants.SQL_EXPRESSION_TRUE;
		}
		 
		result;
	}  
}