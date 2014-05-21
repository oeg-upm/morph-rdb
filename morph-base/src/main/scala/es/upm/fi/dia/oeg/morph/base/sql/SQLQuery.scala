package es.upm.fi.dia.oeg.morph.base.sql

import scala.collection.JavaConversions._
import Zql.ZQuery
import org.apache.log4j.Logger
import Zql.ZSelectItem
import Zql.ZFromItem
import Zql.ZExp
import Zql.ZExpression
import Zql.ZOrderBy
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.Constants
import scala.collection.mutable.LinkedList
import Zql.ZUtils
import java.util.Random
import scala.collection.mutable.LinkedHashMap
import Zql.ZGroupBy

class SQLQuery extends ZQuery with IQuery {
	val logger = Logger.getLogger(this.getClass().getName());
	
	var alias:String =null;
	var slice:Long = -1;
	var offset:Long = -1;
//	var databaseType:String=null;
	var distinct = false;
	var comments:String =null;

	this.addSelect(new java.util.Vector[ZSelectItem]());
	this.addFrom(new java.util.Vector[ZFromItem]());

	
	def this(logicalTable:SQLLogicalTable ) = {
		this();
		this.addLogicalTable(logicalTable);
	}

	def this(logicalTable:IQuery ) =  {
		this();
		val joinQuery = new SQLJoinTable(logicalTable);
		this.addFromItem(joinQuery);
	}

	
	def this(logicalTable:ZFromItem ) = {
	  this()
		this.addFromItem(logicalTable);
	}


	def this(zQuery:ZQuery ) = {
	  this();
		ZUtils.addCustomFunction("concat", 2);
		ZUtils.addCustomFunction("substring", 3);
		ZUtils.addCustomFunction("convert", 2);
		ZUtils.addCustomFunction("coalesce", 2);
		ZUtils.addCustomFunction("abs", 1);
		ZUtils.addCustomFunction("lower", 1);

		if(zQuery.getSelect() != null) { this.addSelect(zQuery.getSelect());}
		if(zQuery.getFrom() != null) { this.addFrom(zQuery.getFrom());}
		if(zQuery.getWhere() != null) { this.addWhere(zQuery.getWhere());}
		if(zQuery.getGroupBy() != null) {this.addGroupBy(zQuery.getGroupBy());}

	}
	

	
	
	def addLogicalTables(logicalTables:Iterable[SQLLogicalTable] ) =  {
	  logicalTables.foreach(logicalTable => this.addLogicalTable(logicalTable));
	}
	
	def addLogicalTable(logicalTable:SQLLogicalTable ) =  {
		val result = logicalTable match {
		  case zFromItem:ZFromItem => { this.addFromItem(zFromItem);}
		  case iQuery:IQuery => {
			val joinQuery = new SQLJoinTable(logicalTable);
			this.addFromItem(joinQuery);		    
		  }
		  case _ => {
		    logger.warn("unknown type of logicalTable!");
		  }
		}
		result;
	}
	
	
	def addFromItem(fromItem:ZFromItem ) = {
		if(this.getFrom() == null) {
			this.addFrom(new java.util.Vector[ZFromItem]());
		}
		
		val oldFromItems = this.getFrom();
		oldFromItems.add(fromItem);
	}

	def addFromItems(fromItems:Iterable[ZFromItem] ) = {
		if(this.getFrom() == null) {
			this.addFrom(new java.util.Vector[ZFromItem]());
		}

		val oldFromItems = this.getFrom();
		oldFromItems.addAll(fromItems);
	}

	def addSelectItems(newSelectItems:List[ZSelectItem] ) {
	  newSelectItems.foreach(newSelectItem => this.addSelectItem(newSelectItem));
	}
	
		
	def addSelects(selectItems:java.util.Collection[ZSelectItem] ) : Unit = {
	  selectItems.foreach(selectItem => {this.addSelectItem(selectItem); } )
	}

	def addSelectItem(newSelectItem:ZSelectItem ) : Unit = {
		val oldSelects= this.getSelect();
		val selectItems : Iterable[ZSelectItem]= if(oldSelects == null) {
			this.setSelectItems(Nil);
			Nil;
		} else {
		  this.getSelectItems();
		}

		val newSelectItemAlias = newSelectItem.getAlias();
		newSelectItem.setAlias("");

		var alreadySelected = false;
		for(selectItem <- selectItems) {
			val selectItemAlias = selectItem.getAlias();
			selectItem.setAlias("");
			if(selectItemAlias != null && !selectItemAlias.equals("")) {
				if(selectItemAlias.equalsIgnoreCase(newSelectItemAlias)) {
					alreadySelected = true;
					logger.debug(selectItemAlias + " already selected");
				}
				selectItem.setAlias(selectItemAlias);
			}			
		}

		if(newSelectItemAlias != null && !newSelectItemAlias.equals("")) {
			newSelectItem.setAlias(newSelectItemAlias);
		}

		if(!alreadySelected) {
			oldSelects.asInstanceOf[java.util.Vector[ZSelectItem]].add(newSelectItem);
		}

	}


	def addWhere(newWheres:java.util.Collection[ZExp] ) : Unit = {
	  newWheres.foreach(newWhere => { this.addWhere(newWhere); })
	}

	override def addWhere(newWhere:ZExp ) = {
		val oldWhere = this.getWhere();
		if(newWhere != null) {
			if(oldWhere == null) {
				super.addWhere(newWhere);
			} else {
				val combinedWhere = new ZExpression("AND", oldWhere, newWhere);
				super.addWhere(combinedWhere);
			}
		}
	}

	def cleanupOrderBy() = {
		val orderByConditions = this.getOrderBy();
		
		if(orderByConditions != null) {
			val orderByConditions2 = orderByConditions.map(orderByConditionAux => {
				val orderByCondition = orderByConditionAux.asInstanceOf[ZOrderBy];
				val orderByExp = orderByCondition.getExpression();
				orderByExp match {
				  case orderByConstant:ZConstant => {
					val orderByValue = orderByConstant.getValue();
					if(orderByValue.startsWith(Constants.PREFIX_VAR)) {
						val orderByValue2 = orderByValue.substring(
								Constants.PREFIX_VAR.length(), orderByValue.length());
						val orderByConstant2 = MorphSQLConstant.apply(orderByValue2
						    , orderByConstant.getType(), this.databaseType, null);
						val orderByCondition2 = new ZOrderBy(orderByConstant2);
						orderByCondition2.setAscOrder(orderByCondition.getAscOrder());
						orderByCondition2;				
					} else {
						orderByCondition;
					}				    
				  }
				  case _ => {
					  orderByCondition;
				  }
				}
			}).toList;
			
			this.setOrderBy(orderByConditions2);			
		}

	}

	def cleanupSelectItems() = {
		val selectItems = this.getSelectItems();

		if(selectItems != null) {
			val selectItems2 = selectItems.flatMap(selectItem => {
				if(selectItem.isExpression()) {
					Some(selectItem);
				} else {
				  val alias = selectItem.getAlias();
					val selectItemName = if(alias == null || alias.equals("")) {
						selectItem.getColumn();
					} else {
						selectItem.getAlias();
					}

					if(selectItemName.startsWith(Constants.PREFIX_VAR)) {
						val newSelectItemAlias = selectItemName.substring(
						    Constants.PREFIX_VAR.length, selectItemName.length());
						selectItem.setAlias(newSelectItemAlias);
						Some(selectItem);					
					} else if(selectItemName.startsWith(Constants.PREFIX_LIT)) {
						//do nothing
					  None
					} else if(selectItemName.startsWith(Constants.PREFIX_URI)) {
						//do nothing
					  None
					} else {
						Some(selectItem);
					}					
				}
			});
			
			this.setSelectItems(selectItems2.toList);			
		}
	}


	def clearSelectItems() = {
		val selectItems = this.getSelect();
		if(selectItems != null) {
			selectItems.clear();
		}
		//selectItems = new Vector<ZSelectItem>();
	}

	def generateAlias() : String = {
		//return R2OConstants.VIEW_ALIAS + this.hashCode();
		if(this.alias == null) {
			this.alias = Constants.VIEW_ALIAS + new Random().nextInt(10000);
		}
		this.alias;
	}

//	def getDatabaseType() : String  = {
//		databaseType;
//	}

	override def getSelectItems() : List[ZSelectItem] = {
	  val selectItems = super.getSelect();
	  
		val result = if(selectItems != null) {
		  selectItems.map(selectItem => selectItem.asInstanceOf[ZSelectItem])
		} else {
		  Nil
		}
		
		result.toList;
	}

	def getSelectItemAliases() : List[String] ={
	  val selectItems = this.getSelectItems();
	  
		val result = if(selectItems != null) {
		  selectItems.map(selectItem => { selectItem.getAlias() })
		} else {
		  Nil
		}

		result.toList;
	}


	override def getFrom() : java.util.Vector[ZFromItem] = {
	  val fromItems = super.getFrom().asInstanceOf[java.util.Vector[ZFromItem]];
	  fromItems
	}
	
	def printFrom() : String  = {
		var fromSQL = "";
		val fromItems = this.getFrom();
		if(fromItems != null && fromItems.size() != 0) {
			var i = 0;
			for(mainQueryFromItem <- fromItems) {
			  mainQueryFromItem match {
			    case sqlFromItem:SQLFromItem => {
					var separator = "";
					if(i > 0) {
						separator = ", ";	
					}
					fromSQL += separator + sqlFromItem.print(true);
				} 
			    case joinQuery:SQLJoinTable => {
					val logicalTable = joinQuery.joinSource;

					var separator = "";
					val logicalTableJoinType = joinQuery.joinType;
					if(logicalTableJoinType != null && !logicalTableJoinType.equals("")) {
						separator = " " + logicalTableJoinType + " JOIN ";
					} else {
						if(i > 0) {
							separator = ", ";	
						}
					}

					logicalTable match {
					  case _:SQLFromItem => {
							fromSQL += separator + logicalTable.toString();
						} 
					  case _:IQuery => {
							fromSQL +=  separator + " ( "+ logicalTable.print(false) + " ) " + logicalTable.getAlias();	
						}
					  case _ => { }
					}

					val joinExp = joinQuery.onExpression;
					if(joinExp != null) {
						fromSQL += " ON " + joinExp;
					}

					if(i < fromItems.size() - 1) {
						fromSQL += "\n";
					}

				} 
			    case _ => {
					var separator = "";
					if(i > 0) {
						separator = ", ";	
					}

					var fromItemString = "";
					if(mainQueryFromItem.getSchema() != null) {
						fromItemString += mainQueryFromItem.getSchema() + ".";
					}
					if(mainQueryFromItem.getTable() != null) {
						fromItemString += mainQueryFromItem.getTable() + ".";
					}
					if(mainQueryFromItem.getColumn() != null) {
						fromItemString += mainQueryFromItem.getColumn() + ".";
					}
					fromItemString = fromItemString.substring(0, fromItemString.length()-1);

					var mainQueryFromItemAlias = mainQueryFromItem.getAlias();
					if(mainQueryFromItemAlias != null && mainQueryFromItemAlias.length() > 0) {
						fromItemString += " " + mainQueryFromItem.getAlias();
					}

					fromSQL += separator + fromItemString;
				}			    
			  }

				i = i+1;
			}
		}

		 fromSQL;
	}

	def getOrderByConditions() : List[ZOrderBy] = {
	  val superOrderByList = super.getOrderBy();
	  
	  val result : List[ZOrderBy]= if(superOrderByList != null) {
		  val thisOrderByList = superOrderByList.map(superOrderBy => {
		    superOrderBy.asInstanceOf[ZOrderBy]
		    }
		  ).toList;
		  thisOrderByList
	  } else {
	    Nil
	  }
	  result;
	}
	
	def printOrderBy() : String = {
		var orderBySQL = "";
		for(orderBy <- this.getOrderByConditions()) {
			val orderByExpression = orderBy.getExpression();
			orderBySQL += orderByExpression;
			if(! orderBy.getAscOrder()) {
				orderBySQL += " DESC";
			}
			orderBySQL += ", ";
		}
		orderBySQL = orderBySQL.substring(0, orderBySQL.length() - 2);
		orderBySQL = "ORDER BY " + orderBySQL ;
		return orderBySQL; 
	}

	def setComments(comments:String ) = { this.comments = comments;	}

//	def setDatabaseType(databaseType:String ) = { this.databaseType = databaseType;	}

	def setDistinct(distinct:Boolean ) = { this.distinct = distinct; }

	def setOffset(offset:Long) = {this.offset = offset;}


	
	def setOrderBy(orderByConditions:List[ZOrderBy] ) = {
		if(this.getOrderBy() != null) {
			this.getOrderBy().clear();	
		}

		val newOrderBy = new java.util.Vector[ZOrderBy](orderByConditions);
		this.addOrderBy(newOrderBy);
	}

	
	def setSelectItems(newSelectItems:List[ZSelectItem] ) {
		this.clearSelectItems();

		//USE THIS FOR MAKING SURE NOT ADDING DUPLICATED ELEMENTS
		newSelectItems.foreach(newSelectItem => this.addSelectItem(newSelectItem));
	}

	def setSlice(slice:Long) = {  this.slice = slice; }

	def setWhere(where:ZExp ) = { super.addWhere(where); }

	override def toString() = {
		var result = "";

		if(comments != null) {
			result += "--" + this.comments + "\n";
		}

		//PRINT SELECT
		val thisSelectItems = this.getSelectItems();
		val selectSQL = MorphSQLUtility.printSelectItemsJava(thisSelectItems, this.getDistinct());
		result += selectSQL + "\n";

		//PRINT FROM
		val fromSQL = this.printFrom();
		result += "FROM " + fromSQL + "\n";

		//PRINT WHERE
		if(this.getWhere() != null) {
			var whereSQL = this.getWhere().toString();
			if(whereSQL.startsWith("(") && whereSQL.endsWith(")")) {
				whereSQL = whereSQL.substring(1, whereSQL.length() - 1);
			}

			if(whereSQL.startsWith("(") && whereSQL.endsWith(")")) {
				whereSQL = whereSQL.substring(1, whereSQL.length() - 1);
			}
			whereSQL = whereSQL.replaceAll("\\) AND \\(", " AND ");
			result += "WHERE " + whereSQL + "\n"; 
		}

		//PRINT ORDER BY
		val orderByConditions = this.getOrderBy(); 
		if(orderByConditions != null && orderByConditions.size() > 0) {
			result += this.printOrderBy() + "\n";
		}

		//PRINT GROUP BY
		val groupBy = this.getGroupBy();
		if(groupBy != null) {
			result += groupBy + "\n";
		}

		//PRINT SLICE
		if(this.slice != -1) {
			result += "LIMIT " + this.slice + " ";
		}

		//PRINT OFFSET
		if(this.offset != -1) {
			result += "OFFSET " + this.offset + " ";
		}

		result.trim();
		result
	}

	def setAlias(alias:String ) = { this.alias = alias;	}

	def getAlias() :String = { this.alias; }

	def print(withAlias:Boolean ) : String = {
		val result = if(withAlias) {
			this.toString();
		} else {
			val alias = this.getAlias();
			this.setAlias("");
			val resultAux = this.toString();
			if(alias != null) {
			  this.setAlias(alias);
			}
			resultAux
		}
		result;
	}

	def buildMapOnExp() : Map[ZConstant, ZConstant] = {
		val fromItems = this.getFrom();
		var mapOnExp:Map[ZConstant, ZConstant]  = Map.empty;
		
		for(fromItem <- fromItems) {
			fromItem match {
				case joinTable:SQLJoinTable => {
					val joinSource = joinTable.joinSource;
					joinSource match {
						case joinSourceSQLQuery:SQLQuery => {
						val joinSourceAlias = joinSourceSQLQuery.getAlias();
						val innerSelectItems = joinSourceSQLQuery.getSelectItems();
						for(innerSelectItem <- innerSelectItems) {
							val innerItemAliasAux = innerSelectItem.getAlias();;
							val innerItemAlias = 
							  if(innerItemAliasAux == null || innerItemAliasAux.equals("")) {
								innerSelectItem.toString();
							  } else {
								  innerItemAliasAux;
							  }
							
							val outerName = joinSourceAlias + "." + innerItemAlias;
							val outerConstant = new ZConstant(outerName, ZConstant.COLUMNNAME);
							val innerConstant = if(innerSelectItem.isExpression()) {
								val innerSelectItemExpression = innerSelectItem.getExpression();
								innerSelectItemExpression match {
								  case innerConstant:ZConstant => {
								    innerConstant
								  } 
								  case _ => {
								    null
								  }
								}
							} else {
								val innerTable = innerSelectItem.getTable();
								val innerColumn = innerSelectItem.getColumn();
								new ZConstant(innerTable + "." + innerColumn, ZConstant.COLUMNNAME);							
							}
	
							if(innerConstant != null) {
								mapOnExp.put(outerConstant, innerConstant);	
							}
	
						}
					}
					case _ => {
						logger.debug("joinSource not SQLQuery!");
					}				  

				}
		    }
		  }
		}

		mapOnExp;

	}
	


//	public Map<ZSelectItem, ZSelectItem> buildMapSelectItemOrigin() {
//
//		Vector<ZFromItem> fromItems = this.getFrom();
//		Map<String, ZSelectItem> mapInnerAliasSelectItem = new HashMap<String, ZSelectItem>();
//		for(ZFromItem fromItem : fromItems) {
//			if(fromItem instanceof SQLFromItem) {
//
//			} else if(fromItem instanceof SQLJoinTable) {
//				SQLJoinTable joinTable = (SQLJoinTable) fromItem;
//				SQLLogicalTable joinSource = joinTable.getJoinSource();
//				if(joinSource instanceof SQLQuery) {
//					SQLQuery joinSourceSQLQuery = (SQLQuery) joinSource;
//					String joinSourceAlias = joinSourceSQLQuery.getAlias();
//
//					Collection<ZSelectItem> innerSelectItems = joinSourceSQLQuery.getSelectItems();
//					for(ZSelectItem innerSelectItem : innerSelectItems) {
//						String innerSelectItemAlias = innerSelectItem.getAlias();
//						if(innerSelectItemAlias == null || innerSelectItemAlias.equals("")) {
//							innerSelectItemAlias = innerSelectItem.toString();
//						}
//						innerSelectItemAlias = joinSourceAlias + "." + innerSelectItemAlias; 
//						mapInnerAliasSelectItem.put(alias.trim(), innerSelectItem);
//					}
//				} else {
//					logger.debug("joinSource not SQLQuery!");
//				}
//			}
//		}
//
//		Map<ZSelectItem, ZSelectItem> result = new HashMap<ZSelectItem, ZSelectItem>();
//
//		Vector<ZSelectItem> outerSelectItems = this.getSelect();
//		for(ZSelectItem outerSelectItem : outerSelectItems) {
//			String outerSelectItemTable = outerSelectItem.getTable();
//			if(outerSelectItemTable == null) {
//				result.put(outerSelectItem, outerSelectItem);
//			} else {
//				String outerSelectItemAlias = outerSelectItem.getAlias();
//				outerSelectItem.setAlias("");
//				String outerSelectItemString = outerSelectItem.toString().trim(); 
//				ZSelectItem innerSelectItem = mapInnerAliasSelectItem.get(outerSelectItemString);
//				if(innerSelectItem != null) {
//					result.put(outerSelectItem, innerSelectItem);
//				}
//				if(outerSelectItemAlias != null) {
//					outerSelectItem.setAlias(outerSelectItemAlias);
//				}
//			}
//		}
//
//		return result;
//	}

	def setFromItems(fromItems:java.util.Collection[ZFromItem] ) = {
		this.addFrom(new java.util.Vector[ZFromItem](fromItems));
	}

//	public boolean hasSubquery() {
//		boolean subqueryFound = false;
//
//		Vector<ZFromItem> fromItems = this.getFrom();
//		for(ZFromItem fromItem : fromItems) {
//			if(!subqueryFound) {
//				if(fromItem instanceof SQLFromItem) {
//					//not a subquery
//				} else if(fromItem instanceof SQLJoinTable) {
//					SQLJoinTable joinTable = (SQLJoinTable) fromItem;
//					SQLLogicalTable joinSource = joinTable.getJoinSource();
//					if(joinSource instanceof SQLFromItem) {
//						//not a subquery
//					} else {
//						subqueryFound = true; 
//					}
//				}				
//			}
//		}
//
//		return subqueryFound;
//	}

	def buildMapAliasSelectItem() : Map[String, ZSelectItem] ={
		SQLQuery.buildMapAliasSelectItemAux(this.alias, this.getSelectItems());
	}



	def pushProjectionsDown(pushedProjections:List[ZSelectItem] 
			, mapInnerAliasSelectItem:Map[String, ZSelectItem] ) {
		val selectItemsReplacement = pushedProjections.map(outerSelectItem => {
			val outerSelectItemTable = outerSelectItem.getTable();
			if(outerSelectItemTable == null) {
				(outerSelectItem -> outerSelectItem);
			} else {
				val outerSelectItemAlias = outerSelectItem.getAlias();
				val outerSelectItemString = MorphSQLSelectItem.print(outerSelectItem, false, false);
				val innerSelectItem = mapInnerAliasSelectItem.get(outerSelectItemString);
				
				val replacementSelectItem = if(innerSelectItem.isDefined) {
					val replacementSelectItemAux = MorphSQLSelectItem.apply(
					    innerSelectItem.get, this.databaseType);
					if(outerSelectItemAlias != null) {
						replacementSelectItemAux.setAlias(outerSelectItemAlias);
					}
					replacementSelectItemAux;
				} else {
					outerSelectItem;
				}
				(outerSelectItem -> replacementSelectItem);
			}
		})


		val newSelectItems = selectItemsReplacement.toMap.values.toList
		this.setSelectItems(newSelectItems);
	}

	def pushExpDown(pushedExp:ZExp , mapInnerAliasSelectItem:Map[String, ZSelectItem] ) 
	: ZExp = {
		val result = if(pushedExp == null) {
		  pushedExp
		} else {
			val dbType = this.getDatabaseType();
			
			val whereReplacement= mapInnerAliasSelectItem.keys.map(alias => {
				val aliasColumn = new ZConstant(alias, ZConstant.COLUMNNAME);
				val selectItem = mapInnerAliasSelectItem(alias);
				val zConstant = if(selectItem.isExpression()) {
					val selectItemValue = selectItem.getExpression().toString();
					new ZConstant(selectItemValue, ZConstant.UNKNOWN);
				} else {
					val selectItemTable = selectItem.getTable();
					val selectItemColumn = selectItem.getColumn();
					val selectItemValue = if(selectItemTable != null && !selectItemTable.equals("")) {
						selectItemTable + "." + selectItemColumn;  
					} else {
						selectItemColumn; 
					}
					MorphSQLUtility.createConstant(selectItemValue, ZConstant.COLUMNNAME, dbType);
				}
				(aliasColumn -> zConstant);
			}).toMap;
	
			val newFilter = MorphSQLUtility.replaceExp(pushedExp, whereReplacement);
			newFilter;		  
		}
		result
	}

	def getDistinct() : Boolean = { this.distinct; }



	//	public static SQLQuery createAux(Collection<ZSelectItem> selectItems
	//			, SQLQuery leftTable, SQLQuery rightTable, ZExpression onExpression) {
	//		SQLQuery result = new SQLQuery();
	//		
	//		Vector<ZFromItem> fromItemsLeft = leftTable.getFrom();
	//		result.addFromItems(fromItemsLeft);
	//		Vector<ZFromItem> fromItemsRight = rightTable.getFrom();
	//		result.addFromItems(fromItemsRight);
	//
	//		result.addWhere(leftTable.getWhere());
	//		result.addWhere(rightTable.getWhere());
	//		
	//		Collection<ZSelectItem> selectItemsLeft = leftTable.getSelectItems();
	//		result.addSelects(selectItemsLeft);
	//		Collection<ZSelectItem> selectItemsRight = rightTable.getSelectItems();
	//		result.addSelects(selectItemsRight);
	//		
	//		Map<String, ZSelectItem> mapAliasSelectItemsLeft = leftTable.buildMapAliasSelectItem();
	//		Map<String, ZSelectItem> mapAliasSelectItemsRight = rightTable.buildMapAliasSelectItem();
	//		Map<String, ZSelectItem> mapAliasSelectItems = 
	//				new LinkedHashMap<String, ZSelectItem>(mapAliasSelectItemsLeft);
	//		mapAliasSelectItems.putAll(mapAliasSelectItemsRight);
	//		result.pushProjectionsDown(selectItems, mapAliasSelectItems);
	//		
	//		result.pushFilterDown(onExpression, mapAliasSelectItems);
	//		
	//		return result;
	//	}



	def pushProjectionsDown(pushedProjections:List[ZSelectItem] ) : Unit = {
		val mapInnerAliasSelectItem = this.buildMapAliasSelectItem();
		this.pushProjectionsDown(pushedProjections, mapInnerAliasSelectItem);
	}

	def pushFilterDown(pushedFilter:ZExp ) : Unit = {
		val newFilter = this.pushExpDown(pushedFilter);
		this.addWhere(newFilter);
	}

	
	def pushExpDown(oldExp:ZExp ) :ZExp ={
		val mapInnerAliasSelectItem = this.buildMapAliasSelectItem();
		val newFilter = this.pushExpDown(oldExp, mapInnerAliasSelectItem);
		newFilter;
	}

	def pushOrderByDown(pushedProjections:List[ZSelectItem] ) = {
		val orderBy = this.getOrderByConditions();
		if(orderBy != null) {
			val mapInnerAliasSelectItem = this.buildMapAliasSelectItem();
			val newOrderByCollection = MorphSQLUtility.pushOrderByDown(
			    orderBy, mapInnerAliasSelectItem);
			this.setOrderBy(newOrderByCollection);			
		}
	}


	
	def pushGroupByDown() = {
		val oldGroupBy = this.getGroupBy();
		if(oldGroupBy != null) {
			val oldGroupByExps = oldGroupBy.getGroupBy().asInstanceOf[Iterable[ZExp]];
			val newGroupByExpsAux = oldGroupByExps.map(oldGroupByExp => {
				val newGroupByExp = this.pushExpDown(oldGroupByExp);
				newGroupByExp;
			});
			val newGroupByExps = new java.util.Vector[ZExp](newGroupByExpsAux);
			val newGroupBy = new ZGroupBy(newGroupByExps);
			this.setGroupBy(newGroupBy);			
		}

	}

	def setGroupBy(newGroupBy:ZGroupBy ) = { this.addGroupBy(newGroupBy); }


}

object SQLQuery {
	val logger = Logger.getLogger(this.getClass().getName());
		
  	def areBaseTables(fromItems:Iterable[ZFromItem] ) : Boolean ={
		var result = true;
		for(fromItem <- fromItems) {
			if(! (fromItem.isInstanceOf[SQLFromItem])) {
				result = false;
			}
		}
		return result;
	}
  	
	def buildMapAliasSelectItemAux(prefix:String , innerSelectItems:Iterable[ZSelectItem] ) 
	: Map[String, ZSelectItem]  = {
		val mapInnerAliasSelectItem = innerSelectItems.map(innerSelectItem => {
			val innerSelectItemAlias = innerSelectItem.getAlias();
			val newColumnNameAux = if(innerSelectItemAlias == null || innerSelectItemAlias.equals("")) {
				innerSelectItem.getColumn();
			} else {
				innerSelectItemAlias;
			}
			val newColumnName = prefix + "." + newColumnNameAux; 
			(newColumnName.trim() -> innerSelectItem);
		})
		
		mapInnerAliasSelectItem.toMap;

	}
	
	def create(selectItems:List[ZSelectItem], leftTable:SQLLogicalTable 
	    , rightTable:SQLLogicalTable , joinType:String , oldJoinExpression:ZExpression 
	    , databasetype:String ) : SQLQuery  ={
		
		var proceed = true;
		if(Constants.JOINS_TYPE_LEFT.equals(joinType) && rightTable.isInstanceOf[SQLQuery]) {
		  rightTable match {
		    case rightTableSQLQuery:SQLQuery => {
				val rightTableFromItems = rightTableSQLQuery.getFrom();
				if(rightTableFromItems.size() != 1) {
					val errorMessage = "Subquery elimination for left outer join can deal with 1 right table only!";
					logger.debug(errorMessage);
					proceed = false;
				}		      
		    }
		    case _ => {}
		  }
		}

		if(proceed) {
			val result = new SQLQuery();
			result.setDatabaseType(databasetype);
			var mapAliasSelectItems:Map[String, ZSelectItem]  = Map.empty; 
			var addedTableAlias:List[String] = Nil;
			
			leftTable match {
			  case leftTableSQLQuery:SQLQuery => {
					val leftTableFromItems = leftTableSQLQuery.getFrom();
					addedTableAlias = leftTableFromItems.map(leftTableFromItem => {
						val fromItemAlias = leftTableFromItem match {
						  case leftTableFromItemJoinTable:SQLJoinTable => {
								leftTableFromItemJoinTable.joinSource.getAlias();
							} 
						  case _ => {
								leftTableFromItem.getAlias();	
							}
						} 
						
						if(fromItemAlias == null || fromItemAlias.equals("")) {
							val logicalTableAlias = leftTable.getAlias();
							if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
								leftTableFromItem.setAlias(logicalTableAlias);	
							}
						}
						
						fromItemAlias;
					}).toList;
					
					leftTableFromItems.foreach(leftTableFromItem => 
					  result.addFromItem(leftTableFromItem));
	
					val leftTableWhere = leftTableSQLQuery.getWhere();
					result.addWhere(leftTableWhere);
					val leftTableSelectItems = leftTableSQLQuery.getSelectItems();
					result.addSelects(leftTableSelectItems);
					val mapAliasSelectItemsLeft = leftTableSQLQuery.buildMapAliasSelectItem();
					mapAliasSelectItems = mapAliasSelectItems ++ mapAliasSelectItemsLeft;
				} 
			  case leftTableFromItem:SQLFromItem => {
					result.addFromItem(leftTableFromItem);
					val addedTableAlias = List(leftTableFromItem.getAlias());
				}
			}

			rightTable match {
				case rightTableSQLQuery:SQLQuery => {
					val rightTableSelectItems = rightTableSQLQuery.getSelectItems();
					val rightTableWhere = rightTableSQLQuery.getWhere();
	
					val mapAliasSelectItemsRight = rightTableSQLQuery.buildMapAliasSelectItem();
					mapAliasSelectItems = mapAliasSelectItems ++ (mapAliasSelectItemsRight);
					val newJoinExpression = result.pushExpDown(oldJoinExpression
					    , mapAliasSelectItems).asInstanceOf[ZExpression];
					val newWhereExpression = result.pushExpDown(rightTableWhere
					    , mapAliasSelectItems).asInstanceOf[ZExpression];
	
					val logicalTableAlias = rightTable.getAlias();
					val rightTableFromItems = rightTableSQLQuery.getFrom();
					
					val joinTables = SQLQuery.generateJoinTables(rightTableFromItems
					    , addedTableAlias, joinType, newJoinExpression, newWhereExpression
					    , logicalTableAlias);
					
					for(joinTable <- joinTables) {
						result.addFromItem(joinTable);
					}
					result.addSelects(rightTableSelectItems);
					result.pushProjectionsDown(selectItems, mapAliasSelectItems);
				} 
				case leftTableFromItem:SQLFromItem => {
					result.addFromItem(leftTableFromItem);
				}			  
			}
			result;
		} else {
		  null
		}
	}

	def create(selectItems:Iterable[ZSelectItem] 
	, sqlLogicalTables:Iterable[_ <: SQLLogicalTable], whereExpression:ZExpression 
	, databasetype:String ) : SQLQuery = {
		var result = new SQLQuery();
		result.setDatabaseType(databasetype);

		var mapAliasSelectItems:Map[String, ZSelectItem] = Map.empty;

		for(sqlLogicalTable <- sqlLogicalTables) {
		  sqlLogicalTable match {
		    case logicalTableSQLQuery:SQLQuery => {
				val logicalTableFromItems = logicalTableSQLQuery.getFrom();
				val resultFromItems = logicalTableFromItems.map (fromItem => {
					val fromItemAlias = fromItem.getAlias();
					if(fromItemAlias == null || fromItemAlias.equals("")) {
						val logicalTableAlias = sqlLogicalTable.getAlias();
						if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
							fromItem.setAlias(logicalTableAlias);	
						}
					}
					fromItem;	
				})
				result.addFromItems(resultFromItems)

				result.addWhere(logicalTableSQLQuery.getWhere());
				
				val selectItemsLeft = logicalTableSQLQuery.getSelectItems();
				result.addSelects(selectItemsLeft);
				
				val mapAliasSelectItemsLeft = logicalTableSQLQuery.buildMapAliasSelectItem();
				mapAliasSelectItems = mapAliasSelectItems ++ mapAliasSelectItemsLeft;		      
		    }
		    case leftTableFromItem:SQLFromItem => {
		      result.addFromItem(leftTableFromItem);
		    }
		    case _ => {}
		  }
		}

		result.pushProjectionsDown(selectItems.toList, mapAliasSelectItems);
		val newExpression = result.pushExpDown(whereExpression, mapAliasSelectItems);
		result.addWhere(newExpression);
		result;

	}
	
	private def generateJoinTables(fromItems:Iterable[ZFromItem]
	, addedTableAlias:List[String] , joinType:String , joinExpression:ZExpression 
	, whereExpression:ZExpression, logicalTableAlias:String ) : Iterable[SQLJoinTable] = {
		val result = SQLQuery.generateJoinTablesAux(fromItems, addedTableAlias, 
				joinType, joinExpression, whereExpression, logicalTableAlias, Set.empty);
		result;
	}
	
	private def generateJoinTablesAux(fromItems:Iterable[ZFromItem] 
	, pAddedTableAlias:List[String] , joinType:String , joinExpression:ZExpression 
	, whereExpression:ZExpression , logicalTableAlias:String 
	, pAddedExpressions:Set[ZExp] ) : Iterable[SQLJoinTable] = {
		var addedExpression = pAddedExpressions;
		var addedTableAlias = pAddedTableAlias;

		val joinTables = fromItems.flatMap(rightTableFromItem => {
			val joinTable = rightTableFromItem match {
				case sqlFromItem:SQLFromItem => {
					val rightTableAlias = rightTableFromItem.getAlias();
					val tableType = sqlFromItem.form;
					val dbType = sqlFromItem.databaseType;
					if(tableType == Constants.LogicalTableType.TABLE_NAME) {
						val rightTableName = rightTableFromItem.getTable();
						val rightTableLogicalTable = new SQLFromItem(
								rightTableName, Constants.LogicalTableType.TABLE_NAME);
						rightTableLogicalTable.databaseType = dbType;
						
						//be careful so that we don't return those join expressions that is not in rightTableAlias
						val relevantJoinExpression1 = MorphSQLUtility.containedInPrefixes(
						    joinExpression, addedTableAlias, true).toSet;
						addedTableAlias = addedTableAlias  ::: List(rightTableAlias);
						
						val relevantJoinExpression2 = MorphSQLUtility.containedInPrefixes(
						    joinExpression, addedTableAlias, true).toSet;
						val relevantJoinExpressions = relevantJoinExpression2.diff(relevantJoinExpression1);
						
						val relevantWhereExpression = MorphSQLUtility.containedInPrefix(
						    whereExpression, rightTableAlias);
						
						val joinTableAux = if(relevantJoinExpressions.isEmpty() && relevantWhereExpression.isEmpty()) {
							new SQLJoinTable(rightTableLogicalTable, joinType
								, Constants.SQL_EXPRESSION_TRUE);
						} else {
						  var combinedExpressionCollection:Set[ZExpression] = Set.empty
						  relevantJoinExpressions.foreach(relevantJoinExpression => {
						    if(!addedExpression.contains(relevantJoinExpression)) {
						      combinedExpressionCollection = combinedExpressionCollection ++ Set(relevantJoinExpression)
						      addedExpression = addedExpression ++ Set(relevantJoinExpression);
						    }
						  })
						  combinedExpressionCollection = combinedExpressionCollection ++ relevantWhereExpression; 
						  val combinedExpressions = MorphSQLUtility.combineExpresions(
									combinedExpressionCollection, Constants.SQL_LOGICAL_OPERATOR_AND);
							new SQLJoinTable(rightTableLogicalTable, joinType, combinedExpressions);	
						}
	
	
						val fromItemAlias = rightTableFromItem.getAlias();
						if(fromItemAlias == null || fromItemAlias.equals("")) {
							if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
								rightTableLogicalTable.setAlias(logicalTableAlias);	
							}
						} else {
							rightTableLogicalTable.setAlias(fromItemAlias);
						}
						
						Some(joinTableAux)
					} else {
					  None
					}
				} 
				case joinTableAux:SQLJoinTable => {
					val rightTableAlias = joinTableAux.joinSource.getAlias();
					
	
					val relevantJoinExpression1 = MorphSQLUtility.containedInPrefixes(
					    joinExpression, addedTableAlias, true).toSet;
					addedTableAlias = addedTableAlias ::: List(rightTableAlias);

					val relevantJoinExpression2 = MorphSQLUtility.containedInPrefixes(
					    joinExpression, addedTableAlias, true).toSet;
					//so that we don't return those join expressions that is not in rightTableAlias
					
					val relevantJoinExpression = relevantJoinExpression2.diff(relevantJoinExpression1)
	
					val relevantWhereExpression = MorphSQLUtility.containedInPrefix(
					    whereExpression, rightTableAlias);
					val combinedExpressionCollection = relevantJoinExpression ++ relevantWhereExpression; 
					val combinedExpressions = MorphSQLUtility.combineExpresions(
							combinedExpressionCollection, Constants.SQL_LOGICAL_OPERATOR_AND);
					joinTableAux.addOnExpression(combinedExpressions);
					Some(joinTableAux)
				}
				case _ => None
			}
			joinTable
		});
		
		return joinTables;
	}
	
	def createQuery(mainTable:SQLLogicalTable , joinTables:Iterable[SQLJoinTable] 
	, selectItems:List[ZSelectItem], whereCondition:ZExpression , databaseType:String ) 
	: SQLQuery = {
		val joinTablesLogicalTables = if(joinTables != null ) {
			joinTables.map(alphaPredicateObject => { alphaPredicateObject.joinSource })
		} else { Nil }
		val joinTablesExpressions = if(joinTables != null) {
			joinTables.map(alphaPredicateObject => { alphaPredicateObject.onExpression })
		} else { Nil}
		val expressionsList = List(whereCondition) ::: joinTablesExpressions.toList
		
		val result = mainTable match {
		  case mainTableSQLQuery:SQLQuery => {
			  val resultAux = mainTableSQLQuery;
			  resultAux.addLogicalTables(joinTablesLogicalTables);
			  val joinExpressions = if(joinTables != null) {
					joinTables.map(alphaPredicateObject => {alphaPredicateObject.onExpression;})
				} else { Nil; }
	
	
				//ZExpression pushedCondSQL = (ZExpression) resultAux.pushExpressionDown(condSQL);
				
				val newWhere = MorphSQLUtility.combineExpresions(expressionsList
						, Constants.SQL_LOGICAL_OPERATOR_AND);
				val pushedNewWhere = resultAux.pushExpDown(newWhere);
				resultAux.addWhere(pushedNewWhere);
				resultAux.pushProjectionsDown(selectItems);
				resultAux
			} 
		  case alphaSubjectFromItem:ZFromItem => {
				val resultAux = new SQLQuery();
				resultAux.addSelectItems(selectItems);
				resultAux.addFromItem(alphaSubjectFromItem);
				if(joinTables != null) {
					for(alphaPredicateObject <- joinTables) {
						resultAux.addFromItem(alphaPredicateObject);
					}				
				}
	
				resultAux.addWhere(whereCondition);
				resultAux
			} 
		  case _ =>{
			  logger.warn("undefined alphasubject type!");
			  val logicalTables = List(mainTable) ::: joinTablesLogicalTables.toList;
			  val newWhere = MorphSQLUtility.combineExpresionsJava(expressionsList
						, Constants.SQL_LOGICAL_OPERATOR_AND);
			  val resultAux = SQLQuery.create(selectItems, logicalTables, newWhere, databaseType);
			  resultAux
			}
		}
		
		result;
	}		

}