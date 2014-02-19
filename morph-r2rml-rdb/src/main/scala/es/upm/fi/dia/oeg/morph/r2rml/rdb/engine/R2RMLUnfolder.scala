package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementVisitor
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder
import java.util.Collection
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.obdi.core.sql.SQLQuery
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLJoinCondition
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import java.util.HashSet
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTable
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSQLQuery
import Zql.ZQuery
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument

class R2RMLUnfolder extends AbstractUnfolder with R2RMLElementVisitor {
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

	def unfold(logicalTable:R2RMLLogicalTable) : SQLLogicalTable = {
	  val logicalTableType = logicalTable.getLogicalTableType();
	  
		val result = logicalTableType match {
		  case LogicalTableType.TABLE_NAME => {
				new SQLFromItem(logicalTable.getValue(), LogicalTableType.TABLE_NAME
				    , this.dbType);
			} 
		  case LogicalTableType.QUERY_STRING => {
				val sqlString = logicalTable.getValue();
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
					new SQLFromItem(sqlString, LogicalTableType.QUERY_STRING, this.dbType);				    
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

	def  unfold(logicalTable:R2RMLLogicalTable, subjectMap:R2RMLSubjectMap
			, poms:Collection[R2RMLPredicateObjectMap] ) : SQLQuery = {
		val triplesMap = subjectMap.getOwner();
		logger.info("unfolding triplesMap : " + triplesMap);

		//unfold subjectMap
		//R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
		val result = this.unfoldSubjectMap(subjectMap, logicalTable);
		val databaseType = triplesMap.getOwner().getConfigurationProperties().databaseType;
		result.setDatabaseType(databaseType);
		
		val logicalTableAlias = logicalTable.getAlias();
		
		if(poms != null) {
			for(predicateObjectMap <- poms) {
				//UNFOLD PREDICATEMAP
				val predicateMaps = predicateObjectMap.getPredicateMaps();
				if(predicateMaps != null && !predicateMaps.isEmpty()) {
					val predicateMap = predicateObjectMap.getPredicateMap(0);
					val predicateMapColumnsString = predicateMap.getDatabaseColumnsString();
					//if(predicateMapColumnsString != null && logicalTable instanceof R2RMLTable) {
					if(predicateMapColumnsString != null) {
						for(predicateMapColumnString <- predicateMapColumnsString) {
							val selectItem = MorphSQLSelectItem.apply(predicateMapColumnString
									, logicalTableAlias, dbType);
							if(selectItem != null) {
								if(selectItem.getAlias() == null) {
									val alias = selectItem.getTable() + "_" + selectItem.getColumn();
									selectItem.setAlias(alias);
									if(this.mapTermMapColumnsAliases.containsKey(predicateMap)) {
										val oldColumnAliases = this.mapTermMapColumnsAliases(predicateMap);
										val newColumnAliases = oldColumnAliases ::: List(alias);
										this.mapTermMapColumnsAliases += (predicateMap -> newColumnAliases);
									} else {
										this.mapTermMapColumnsAliases += (predicateMap -> List(alias));
									}								
								}
								//resultSelectItems.add(selectItem);
								result.addSelect(selectItem);
							}
						}
					}					
				}


				//UNFOLD OBJECTMAP
				val objectMaps = predicateObjectMap.getObjectMaps();
				if(objectMaps != null && !objectMaps.isEmpty()) {
					val objectMap = predicateObjectMap.getObjectMap(0);
					if(objectMap != null) {
						//objectMap.setAlias(logicalTableAlias);
						val objectMapColumnsString = objectMap.getDatabaseColumnsString();
						//if(objectMapColumnsString != null && logicalTable instanceof R2RMLTable) {
						if(objectMapColumnsString != null) {
							for(objectMapColumnString <- objectMapColumnsString) {
								val selectItem = MorphSQLSelectItem(objectMapColumnString
								    , logicalTableAlias, this.dbType);
								if(selectItem != null) {
									if(selectItem.getAlias() == null) {
										val alias = selectItem.getTable() + "_" + selectItem.getColumn();
										selectItem.setAlias(alias);
										if(this.mapTermMapColumnsAliases.containsKey(objectMap)) {
											val oldColumnAliases = this.mapTermMapColumnsAliases(objectMap);
											val newColumnAliases = oldColumnAliases ::: List(alias);
											this.mapTermMapColumnsAliases += (objectMap -> newColumnAliases);
										} else {
											this.mapTermMapColumnsAliases += (objectMap -> List(alias));
										}
									}
									//resultSelectItems.add(selectItem);
									result.addSelect(selectItem);
								}
							}
						}
					}					
				}


				//UNFOLD REFOBJECTMAP
				val refObjectMaps = predicateObjectMap.getRefObjectMaps();
				if(refObjectMaps != null && !refObjectMaps.isEmpty()) {
					val refObjectMap = predicateObjectMap.getRefObjectMap(0);
					if(refObjectMap != null) {
						val parentLogicalTable = refObjectMap.getParentLogicalTable();
						if(parentLogicalTable == null) {
							val errorMessage = "Parent logical table is not found for RefObjectMap : " + predicateObjectMap.getMappedPredicateName(0);
							throw new Exception(errorMessage);
						}
						val sqlParentLogicalTable = this.unfold(parentLogicalTable);
								
						val joinQuery = new SQLJoinTable(sqlParentLogicalTable);
						joinQuery.setJoinType("LEFT");
						val joinQueryAlias = sqlParentLogicalTable.generateAlias();
						sqlParentLogicalTable.setAlias(joinQueryAlias);
						//refObjectMap.setAlias(joinQueryAlias);
						this.mapRefObjectMapAlias += (refObjectMap -> joinQueryAlias);
						predicateObjectMap.setAlias(joinQueryAlias);
						
						val refObjectMapColumnsString = 
								refObjectMap.getParentDatabaseColumnsString();
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
								result.addSelect(selectItem);
							}
						}


						
						val joinConditions = refObjectMap.getJoinConditions();
						val onExpression = if(joinConditions != null && joinConditions.size() > 0) {
							R2RMLJoinCondition.generateJoinCondition(joinConditions
									, logicalTableAlias, joinQueryAlias, dbType);
						} else {
							Constants.SQL_EXPRESSION_TRUE;
						}
						joinQuery.setOnExpression(onExpression);
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
	
	def unfold(triplesMap:R2RMLTriplesMap , subjectURI:String ) : SQLQuery  = {
		val logicalTable = triplesMap.getLogicalTable();
		val subjectMap = triplesMap.getSubjectMap();
		val predicateObjectMaps = triplesMap.getPredicateObjectMaps();
		
		val resultAux = this.unfold(logicalTable, subjectMap, predicateObjectMaps);
		val result = if(subjectURI != null) {
			val whereExpression = subjectMap.generateCondForWellDefinedURI(subjectURI, logicalTable.getAlias(), this.dbType);
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
	
	def unfold(triplesMap:R2RMLTriplesMap ) : SQLQuery  = {
		this.unfold(triplesMap, null);
	}

	override def unfoldConceptMapping(cm:AbstractConceptMapping ) : SQLQuery  = {
		this.unfold(cm.asInstanceOf[R2RMLTriplesMap]);
	}
	
	override def unfoldConceptMapping(cm:AbstractConceptMapping,subjectURI:String):SQLQuery={
		this.unfold(cm.asInstanceOf[R2RMLTriplesMap], subjectURI);
	}

	override def unfoldMappingDocument(mappingDocument:AbstractMappingDocument ) 
	: Collection[SQLQuery] = {
		var result:Collection[SQLQuery] = new HashSet[SQLQuery]();

		val triplesMaps = mappingDocument.getConceptMappings();
		if(triplesMaps != null) {
			for(triplesMap <- triplesMaps) {
				try {
					val triplesMapUnfolded = this.unfoldConceptMapping(triplesMap);
					result.add(triplesMapUnfolded);
				} catch {
				  case e:Exception => {
					logger.error("error while unfolding triplesMap : " + triplesMap);
					logger.error("error message = " + e.getMessage());				    
				  }
				}
			}
		}
		result;
	}

	override def unfoldSubject(cm:AbstractConceptMapping) : SQLQuery = {
		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val logicalTable = triplesMap.getLogicalTable();
		val subjectMap = triplesMap.getSubjectMap();
		val predicateObjectMaps = triplesMap.getPredicateObjectMaps();
		val result = this.unfold(logicalTable, subjectMap, null);
		return result;
	}

	def unfoldSubjectMap(subjectMap:R2RMLSubjectMap, logicalTable:R2RMLLogicalTable):SQLQuery={
//		R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
//		R2RMLSubjectMap subjectMap = triplesMap.getSubjectMap();
		
		val result = new SQLQuery();
		val resultSelectItems = new HashSet[ZSelectItem]();

		val logicalTableUnfolded :SQLFromItem = logicalTable match {
		  case _:R2RMLTable => {
				this.unfold(logicalTable).asInstanceOf[SQLFromItem];
			} 
		  case _:R2RMLSQLQuery => {
				val logicalTableAux = logicalTable.accept(this);
				logicalTableAux match {
				  case _:SQLQuery => {
						val zQuery = this.unfold(logicalTable).asInstanceOf[ZQuery];
						new SQLFromItem(zQuery.toString(), LogicalTableType.QUERY_STRING, this.dbType);
					} 
				  case sqlFromItem:SQLFromItem => { sqlFromItem; }
				  case _ => { null}
				}
			}
		  case _ => {null}
		}
		
		val logicalTableAlias = logicalTableUnfolded.generateAlias();
		logicalTable.setAlias(logicalTableAlias);
		//result.addFrom(logicalTableUnfolded);
		val logicalTableUnfoldedJoinTable = new SQLJoinTable(logicalTableUnfolded, null, null); 
		result.addFromItem(logicalTableUnfoldedJoinTable);

		val subjectMapColumnsString = subjectMap.getDatabaseColumnsString();
		if(subjectMapColumnsString != null) {
			for(subjectMapColumnString <- subjectMapColumnsString) {
				val selectItem = MorphSQLSelectItem.apply(subjectMapColumnString, logicalTableAlias, this.dbType);
				
				if(selectItem != null) {
					if(selectItem.getAlias() == null) {
						val alias = selectItem.getTable() + "_" + selectItem.getColumn();
						if(this.mapTermMapColumnsAliases.containsKey(subjectMap)) {
							val oldColumnAliases = this.mapTermMapColumnsAliases(subjectMap);
							val newColumnAliases = oldColumnAliases ::: List(alias);
							this.mapTermMapColumnsAliases += (subjectMap -> newColumnAliases);
						} else {
							this.mapTermMapColumnsAliases += (subjectMap -> List(alias));
						}
						
						selectItem.setAlias(alias);						
					}
					resultSelectItems.add(selectItem);
				}
			}
		}
		
		result.setSelectItems(resultSelectItems);
		result;
	}

	def visit(logicalTable:R2RMLLogicalTable ) : SQLLogicalTable  ={
		val result = this.unfold(logicalTable);
		result;
	}

	def visit( mappingDocument:AbstractMappingDocument) : Collection[SQLQuery] = {
		val  result = this.unfoldMappingDocument(mappingDocument);
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

	def visit(triplesMap:R2RMLTriplesMap ) : SQLQuery  = {
		val result = this.unfold(triplesMap);
		result;
	}
}