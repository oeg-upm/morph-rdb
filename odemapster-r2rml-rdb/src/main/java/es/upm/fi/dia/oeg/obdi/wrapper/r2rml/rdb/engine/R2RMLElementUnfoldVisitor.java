package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import Zql.ZExpression;
import Zql.ZQuery;
import Zql.ZSelectItem;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.ILogicalQuery;
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLQuery;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.R2RMLUtility;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLJoinCondition;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLMappingDocument;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSQLQuery;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSubjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTable;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem;

public class R2RMLElementUnfoldVisitor extends AbstractUnfolder implements R2RMLElementVisitor {
	private Map<Object, Collection<String>> mapTermMapColumnsAliases = new HashMap<Object, Collection<String>>();
	
	public R2RMLElementUnfoldVisitor() {
		super();
	}

	private static Logger logger = Logger.getLogger(R2RMLElementUnfoldVisitor.class);
	private Map<R2RMLRefObjectMap, String> mapRefObjectMapAlias = new HashMap<R2RMLRefObjectMap, String>();
	//private ConfigurationProperties configurationProperties;
	
	public Map<R2RMLRefObjectMap, String> getMapRefObjectMapAlias() {
		return mapRefObjectMapAlias;
	}

	public Collection<SQLQuery> visit(R2RMLMappingDocument mappingDocument) {
		Collection<SQLQuery> result = new HashSet<SQLQuery>();

		Collection<AbstractConceptMapping> triplesMaps = mappingDocument.getConceptMappings();
		if(triplesMaps != null) {
			for(AbstractConceptMapping triplesMap : triplesMaps) {
				try {
					SQLQuery triplesMapUnfolded = (SQLQuery) ((R2RMLTriplesMap) triplesMap).accept(this);
					result.add(triplesMapUnfolded);
				} catch(Exception e) {
					logger.error("error while unfolding triplesMap : " + triplesMap);
					logger.error("error message = " + e.getMessage());
				}
			}
		}
		return result;
	}

	private SQLQuery unfoldSubjectMap(R2RMLTriplesMap triplesMap) {
		R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
		R2RMLSubjectMap subjectMap = triplesMap.getSubjectMap();
		SQLQuery result = new SQLQuery();
		Collection<ZSelectItem> resultSelectItems = new HashSet<ZSelectItem>();
		
		SQLFromItem logicalTableUnfolded = null;
		String logicalTableAlias = null;

		if(logicalTable instanceof R2RMLTable) {
			logicalTableUnfolded = (SQLFromItem) logicalTable.accept(this);
		} else if(logicalTable instanceof R2RMLSQLQuery) {
			Object logicalTableAux = logicalTable.accept(this);
			if(logicalTableAux instanceof SQLQuery) {
				ZQuery zQuery = (ZQuery) logicalTable.accept(this);
				logicalTableUnfolded = new SQLFromItem(zQuery.toString(), LogicalTableType.QUERY_STRING, this.dbType);
			} else if(logicalTableAux instanceof SQLFromItem) {
				logicalTableUnfolded = (SQLFromItem) logicalTableAux;
			}
		}
		logicalTableAlias = logicalTableUnfolded.generateAlias();
		logicalTable.setAlias(logicalTableAlias);
		//result.addFrom(logicalTableUnfolded);
		result.addFromItem(new SQLJoinTable(logicalTableUnfolded, null, null));

		Collection<String> subjectMapColumnsString = subjectMap.getDatabaseColumnsString();
		if(subjectMapColumnsString != null) {
			new R2RMLUtility();
			
			for(String subjectMapColumnString : subjectMapColumnsString) {
				ZSelectItem selectItem = MorphSQLSelectItem.apply(subjectMapColumnString, logicalTableAlias, this.dbType);
				
				if(selectItem != null) {
					if(selectItem.getAlias() == null) {
						String alias = selectItem.getTable() + "_" + selectItem.getColumn();
						if(this.mapTermMapColumnsAliases.containsKey(subjectMap)) {
							this.mapTermMapColumnsAliases.get(subjectMap).add(alias);
						} else {
							Collection<String> aliases = new Vector<String>();
							aliases.add(alias);
							this.mapTermMapColumnsAliases.put(subjectMap, aliases);
						}
						
						selectItem.setAlias(alias);						
					}
					resultSelectItems.add(selectItem);
				}
			}
		}
		
		result.setSelectItems(resultSelectItems);
		return result;
	}

	public SQLQuery visit(R2RMLTriplesMap triplesMap) throws Exception {
		logger.info("unfolding triplesMap : " + triplesMap);

		R2RMLSubjectMap subjectMap = triplesMap.getSubjectMap();
		//SQLQuery result = new SQLQuery();
		
		//Collection<ZSelectItem> resultSelectItems = new HashSet<ZSelectItem>();

		//unfold logical table
		R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
		String logicalTableAlias = null;

		//unfold subjectMap
		SQLQuery result = this.unfoldSubjectMap(triplesMap);
		String databaseType = triplesMap.getOwner().getConfigurationProperties().getDatabaseType();
		result.setDatabaseType(databaseType);
		
		//logicalTableAlias = subjectMap.getAlias();
		logicalTableAlias = triplesMap.getLogicalTable().getAlias();

		Collection<R2RMLPredicateObjectMap> predicateObjectMaps = 
				triplesMap.getPredicateObjectMaps();
		if(predicateObjectMaps != null) {
			for(R2RMLPredicateObjectMap predicateObjectMap : predicateObjectMaps) {
				//unfold predicateMap
				R2RMLPredicateMap predicateMap = predicateObjectMap.getPredicateMap();
				Collection<String> predicateMapColumnsString = predicateMap.getDatabaseColumnsString();
				//if(predicateMapColumnsString != null && logicalTable instanceof R2RMLTable) {
				if(predicateMapColumnsString != null) {
					
					for(String predicateMapColumnString : predicateMapColumnsString) {
						ZSelectItem selectItem = MorphSQLSelectItem.apply(predicateMapColumnString
								, logicalTableAlias, dbType);
						if(selectItem != null) {
							if(selectItem.getAlias() == null) {
								String alias = selectItem.getTable() + "_" + selectItem.getColumn();
								selectItem.setAlias(alias);
								if(this.mapTermMapColumnsAliases.containsKey(predicateMap)) {
									this.mapTermMapColumnsAliases.get(predicateMap).add(alias);
								} else {
									Collection<String> aliases = new Vector<String>();
									aliases.add(alias);
									this.mapTermMapColumnsAliases.put(predicateMap, aliases);
								}								
							}
							//resultSelectItems.add(selectItem);
							result.addSelect(selectItem);
						}
					}
				}

				//unfold objectMap
				R2RMLObjectMap objectMap = predicateObjectMap.getObjectMap();
				if(objectMap != null) {
					//objectMap.setAlias(logicalTableAlias);
					Collection<String> objectMapColumnsString = objectMap.getDatabaseColumnsString();
					//if(objectMapColumnsString != null && logicalTable instanceof R2RMLTable) {
					if(objectMapColumnsString != null) {
						for(String objectMapColumnString : objectMapColumnsString) {
							ZSelectItem selectItem = MorphSQLSelectItem.apply(
									objectMapColumnString, logicalTableAlias, this.dbType);
							if(selectItem != null) {
								if(selectItem.getAlias() == null) {
									String alias = selectItem.getTable() + "_" + selectItem.getColumn();
									selectItem.setAlias(alias);
									if(this.mapTermMapColumnsAliases.containsKey(objectMap)) {
										this.mapTermMapColumnsAliases.get(objectMap).add(alias);
									} else {
										Collection<String> aliases = new Vector<String>();
										aliases.add(alias);
										this.mapTermMapColumnsAliases.put(objectMap, aliases);
									}
								}
								//resultSelectItems.add(selectItem);
								result.addSelect(selectItem);
							}
						}
					}
				}

				//unfold refObjectMap
				R2RMLRefObjectMap refObjectMap = predicateObjectMap.getRefObjectMap();
				if(refObjectMap != null) {
					R2RMLLogicalTable parentLogicalTable = refObjectMap.getParentLogicalTable();
					if(parentLogicalTable == null) {
						String errorMessage = "Parent logical table is not found for RefObjectMap : " + predicateObjectMap.getMappedPredicateName();
						throw new Exception(errorMessage);
					}
					SQLLogicalTable sqlParentLogicalTable = 
							(SQLLogicalTable) parentLogicalTable.accept(this);
					
					SQLJoinTable joinQuery = new SQLJoinTable(sqlParentLogicalTable);
					joinQuery.setJoinType("LEFT");
					String joinQueryAlias = sqlParentLogicalTable.generateAlias();
					sqlParentLogicalTable.setAlias(joinQueryAlias);
					//refObjectMap.setAlias(joinQueryAlias);
					this.mapRefObjectMapAlias.put(refObjectMap, joinQueryAlias);
					predicateObjectMap.setAlias(joinQueryAlias);
					
					Collection<String> refObjectMapColumnsString = 
							refObjectMap.getParentDatabaseColumnsString();
					if(refObjectMapColumnsString != null ) {
						for(String refObjectMapColumnString : refObjectMapColumnsString) {
							ZSelectItem selectItem = MorphSQLSelectItem.apply(
									refObjectMapColumnString, joinQueryAlias, dbType, null);
							if(selectItem.getAlias() == null) {
								String alias = selectItem.getTable() + "_" + selectItem.getColumn();
								selectItem.setAlias(alias);
								if(this.mapTermMapColumnsAliases.containsKey(refObjectMap)) {
									this.mapTermMapColumnsAliases.get(refObjectMap).add(alias);
								} else {
									Collection<String> aliases = new Vector<String>();
									aliases.add(alias);
									this.mapTermMapColumnsAliases.put(refObjectMap, aliases);
								}
							}							
							//resultSelectItems.add(selectItem);
							result.addSelect(selectItem);
						}
					}


					ZExpression onExpression;
					Collection<R2RMLJoinCondition> joinConditions = 
							refObjectMap.getJoinConditions();
					if(joinConditions != null && joinConditions.size() > 0) {
						onExpression = R2RMLUtility.generateJoinCondition(joinConditions
								, logicalTableAlias, joinQueryAlias, dbType);
					} else {
						onExpression = Constants.SQL_EXPRESSION_TRUE();
					}
					joinQuery.setOnExpression(onExpression);
					//result.addJoinQuery(joinQuery);		
					result.addFromItem(joinQuery);
				}

			}
		}

//		if(resultSelectItems != null) {
//			for(ZSelectItem selectItem : resultSelectItems) {
//				result.addSelect(selectItem);
//			}
//		}
		logger.info(triplesMap + " unfolded = \n" + result);

		return result;
	}

	public SQLLogicalTable visit(R2RMLLogicalTable logicalTable) {
		SQLLogicalTable result;
		
		Enum<LogicalTableType> logicalTableType = logicalTable.getLogicalTableType();
		if(logicalTableType == LogicalTableType.TABLE_NAME) {
			result = new SQLFromItem(logicalTable.getValue(), LogicalTableType.TABLE_NAME, this.dbType);
		} else if(logicalTableType == LogicalTableType.QUERY_STRING) {
			String sqlString = logicalTable.getValue();
			try {
				String sqlString2 = sqlString;
				if(!sqlString2.endsWith(";")) {
					sqlString2 += ";";
				}
				result = R2RMLUtility.toSQLQuery(sqlString2);
			} catch(Exception e) {
				logger.warn("Not able to parse the query, string will be used.");
				result = new SQLFromItem(sqlString, LogicalTableType.QUERY_STRING, this.dbType);
			}
		} else {
			result = null;
			logger.warn("Invalid logical table type");
		}

		return result;
	}

	public Object visit(R2RMLRefObjectMap refObjectMap) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visit(R2RMLObjectMap objectMap) {
		// TODO Auto-generated method stub
		return null;
	}

	protected Set<String> unfold(Set<ILogicalQuery> logicalQueries,
			AbstractMappingDocument mapping) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String unfoldConceptMapping(AbstractConceptMapping triplesMap)
			throws Exception {
		return this.visit((R2RMLTriplesMap) triplesMap).toString();
	}

	@Override
	protected Collection<String> unfold(AbstractMappingDocument mappingDocument) throws Exception {
		Collection<String> result = new HashSet<String>();
		Collection<SQLQuery> queries = this.visit((R2RMLMappingDocument) mappingDocument);
		for(SQLQuery query : queries) {
			result.add(query.toString());
		}
		return result;
	}

	public Object visit(R2RMLTermMap r2rmlTermMap) {
		// TODO Auto-generated method stub
		return null;
	}

	public Collection<String> getAliases(Object termMapOrRefObjectMap) {
		return this.mapTermMapColumnsAliases.get(termMapOrRefObjectMap);
	}

}
