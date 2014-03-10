package es.upm.fi.dia.oeg.morph.base.sql

import Zql.ZExpression
import Zql.ZConstant
import Zql.ZSelectItem
import scala.collection.JavaConversions._
import Zql.ZExp
import com.hp.hpl.jena.graph.Node
import org.apache.log4j.Logger
import Zql.ZOrderBy
import scala.collection.mutable.LinkedHashMap
import es.upm.fi.dia.oeg.morph.base.Constants
import scala.Option.option2Iterable

class MorphSQLUtility {
	
}

object MorphSQLUtility {
	val logger = Logger.getLogger("MorphSQLUtility");
  
	def createConstant(v:String, typ:Integer, dbType:String) : ZConstant = {
	  val result = {
		dbType match {
	    	case Constants.DATABASE_MONETDB => {
	    		val monetdbColumn = MorphSQLConstant.apply(v, typ, Constants.DATABASE_MONETDB, null);
	    		monetdbColumn
	    	}
	    	case _ => {
	    		new ZConstant(v, typ);
	    	}
		}	    
	  }
	  result
	}
	
	def getSelectItemByAlias(alias:String , selectItems:Iterable[ZSelectItem] , prefix:String ) 
	: Option[ZSelectItem] = {
		var result : Option[ZSelectItem] = None;
		
		if(selectItems != null) {
			for(selectItem <- selectItems) {
			  if(result != null) {
				val selectItemAlias = selectItem.getAlias();
				if(alias.equals(selectItemAlias) || alias.equals(prefix + "." + selectItemAlias)) {
					result = Some(selectItem);
				}				    
			  }
			}
		}
		
		return result;
	}
	
	def addSelectItem(selectItems:Iterable[ZSelectItem] , newSelectItem:ZSelectItem) = {
		val newSelectItemAlias = newSelectItem.getAlias();
		val oldSelectItem = this.getSelectItemByAlias(newSelectItemAlias, selectItems, null);
		if (!oldSelectItem.isDefined) {
			selectItems.add(newSelectItem);
		}
	}

	def addSelectItems(selectItems:Iterable[ZSelectItem] , newSelectItems:Iterable[ZSelectItem]) = {
		for(newSelectItem <- newSelectItems) {
			this.addSelectItem(selectItems, newSelectItem);
		}
	}
	
	def setTableName(selectItems:Iterable[MorphSQLSelectItem] , tableName:String) = {
	  for(oldSelectItem <- selectItems) {
		  val dbType = oldSelectItem.dbType;
	      val columnType = oldSelectItem.columnType;
	  }
	}

//	def combineExpressions(exp1:ZExp , exp2:ZExp,logicalOperator:String ) : ZExpression = {
//		val result:ZExpression = {
//			if(exp1 == null && exp2 == null) {
//				null;
//			} else if(exp1 == null) {
//				exp2 match {
//				  case zExpression:ZExpression => {
//				    zExpression
//				  }
//				  case _ => {
//				    new ZExpression(exp2.toString())
//				  }
//				}
//			} else if (exp2 == null) {
//				exp1 match {
//				  case zExpression:ZExpression => {
//				    zExpression
//				  }
//				  case _ => {
//				    new ZExpression(exp1.toString())
//				  }			  
//				}
//			} else {
//				new ZExpression(logicalOperator, exp1, exp2);	
//			}		  
//		} 
//		
//		result;
//	}
	
	def  combineExpresionsJava(exps:java.util.Collection[_ <: ZExp] , logicalOperator:String ) : ZExpression = {
	  val result = this.combineExpresions(exps.toList, logicalOperator);
	  result
	}
	
	def  combineExpresions(pExps:Iterable[_ <: ZExp] , logicalOperator:String ) : ZExpression = {
		val exps = pExps.flatMap(exp => {
		  if(exp == null) {None}
		  else {Some(exp)}
		});
		val it = exps.iterator;
		
		val result = {
			if(exps.size == 0) {
			  null
			} else if(exps.size == 1) {
				val exp = it.next();
				val resultAux = {
					exp match {
					  case zExpression:ZExpression => { zExpression }
					  case _ => { new ZExpression(exp.toString()); }
					}				  
				}
				resultAux
			} else {
				
				var resultAux = new ZExpression(logicalOperator);
				while(it.hasNext()) {
					resultAux.addOperand(it.next());
				}
				resultAux
			}		  
		}

		result;
	}
	
	def replaceExp(oldExp : ZExp, mapReplacement : Map[ZConstant, ZConstant])  : ZExp= {
		var newExpression : ZExp = null;

		if(mapReplacement.size == 0) { 
			newExpression = oldExp;
		} else {
			val mapReplacementHead = mapReplacement.head;
			val newExpressionAux = this.replaceExp(oldExp, mapReplacementHead);
			newExpression = this.replaceExp(newExpressionAux, mapReplacement.tail);
		}

		newExpression
	}

	private def replaceExp(oldExp : ZExp, replacementTuple : (ZConstant, ZConstant)) : ZExp = {
		val newExp : ZExp = oldExp match {
		  case oldExpression:ZExpression => {
			val oldExpression = oldExp.asInstanceOf[ZExpression];
			val operator = oldExpression.getOperator();
			val oldOperands = oldExpression.getOperands();
			var newOperands = List[ZExp]();

			for(oldOperand <- oldOperands) {
				val newOperand : ZExp = oldOperand match  {
				  case oldOperandConstant:ZConstant => {
				    this.replaceConstant(oldOperandConstant, replacementTuple);
				  }
				  case oldOperandExpression:ZExpression => {
				    this.replaceExp(oldOperandExpression, replacementTuple);
				  }
				  case _ => {
				    null
				  }
				}
				
				newOperands = newOperand :: newOperands; 
			}

			val newExpression = new ZExpression(operator);
			for(newOperand <- newOperands reverse) yield {
				newExpression.addOperand(newOperand);
			}
			newExpression;
		  }
		  case oldConstant:ZConstant => {
		    this.replaceConstant(oldConstant, replacementTuple);
		  }
		  case _ => {
		    oldExp;
		  }
		}

		newExp;
	}

	private def replaceConstant(oldExp : ZConstant, replacementTuple : (ZConstant, ZConstant) ) : ZConstant = {
		val replacementTuple1 =  replacementTuple._1;
		val replacementTuple2 =  replacementTuple._2;

		
		val oldExpValue = MorphSQLUtility.printWithoutEnclosedCharacters(
		    oldExp.getValue().trim());
		val replacedValue = MorphSQLUtility.printWithoutEnclosedCharacters(
		    replacementTuple1.getValue().trim());

		val newConstant : ZConstant = {
			if(oldExpValue.equals(replacedValue)) {
				MorphSQLConstant(replacementTuple2);
			} else {
				oldExp;
			}
		}
		newConstant;
	}
	  
	
	def printSelectItemsJava(selectItems : java.util.Collection[ZSelectItem] , distinct : Boolean ) : String = {
		val result = this.printSelectItems(selectItems, distinct);
		result
	}
	
	def printSelectItems(selectItems : Iterable[ZSelectItem] , distinct : Boolean ) : String = {
		var selectSQL = "SELECT ";
		
		if(distinct) {
			selectSQL += " DISTINCT ";
		}

		if(selectItems != null && selectItems.size()!=0) {
			var selectItemStringList = List[String]();
			
			for(selectItem <- selectItems) {
				val selectItemAlias = selectItem.getAlias();
				selectItem.setAlias("");
				val aggregationFunction = selectItem.getAggregate();
				
				val selectItemString = {
					if(aggregationFunction == null) {
						selectItem.toString();
					} else {
						aggregationFunction + "(" + selectItem.toString() + ") ";
					}				  
				}

				
				val selectItemStringWithAlias = {
					if(selectItemAlias != null && !selectItemAlias.equals("")) {
						selectItem.setAlias(selectItemAlias);
						selectItemString + " AS \"" + selectItemAlias + "\"";
					} else {
						selectItemString
					}				   
				}
				
				selectItemStringList = selectItemStringList ::: List(selectItemStringWithAlias);
			}
			val selectItemStrings = selectItemStringList.mkString(",");
			selectSQL = selectSQL + selectItemStrings;
		} else {
			selectSQL += " * ";
		}

		selectSQL;		
	}

	def areAllConstants(exps:Iterable[ZExp] ) = {
		var result:Boolean = false;
		
		if(exps == null) {
			result = false;
		} else {
			result = true;
			for(exp <- exps) {
				if(!exp.isInstanceOf[ZConstant]) {
				  result = false
				}
			}			
		}
		
		result;
	}

	def containedInPrefixAsJava(exp:ZExp, prefix:String) : java.util.Collection[ZExpression] = {
	  this.containedInPrefix(exp, prefix);
	}
	
	def containedInPrefix(exp:ZExp, prefix:String) : Iterable[ZExpression] = {
		val prefixes = List(prefix);
		val result = MorphSQLUtility.containedInPrefixes(exp, prefixes, true);
		result;
	}
		
	def containedInPrefixesJava(exp:ZExp , prefixes:java.util.Collection[String] , allPrefixes:Boolean ) : java.util.Collection[ZExpression] = {
	  this.containedInPrefixes(exp, prefixes, allPrefixes)
	}
	
	def containedInPrefixes(exp:ZExp , prefixes:Iterable[String] , allPrefixes:Boolean ) 
	: Iterable[ZExpression] = {
		var result : Set[ZExpression]= Set.empty;
		
		exp match {
		  case expExpression:ZExpression => {
			val operands : List[ZExp] = expExpression.getOperands().toList.asInstanceOf[List[ZExp]];
			
			if(MorphSQLUtility.areAllConstants(operands)) {
				var resultAux : List[ZExp]= Nil;
				for(operand <- operands) {
					operand match {
					  case zConstant:ZConstant => {
						if(zConstant.getType() == ZConstant.COLUMNNAME) {
							val operandString = MorphSQLUtility.printWithoutEnclosedCharacters(zConstant.getValue());
							for(prefix <- prefixes) {
								if(operandString.contains(prefix + ".")) {
									resultAux = resultAux ::: List(operand);	
								}						
							}
						} else {
							resultAux = resultAux ::: List(operand);
						}					    
					  } 
					}
				}
				
				if(allPrefixes) {
					if(resultAux.size() == operands.size()) {
						result = result ++ Set(expExpression);
					}					
				} else {
					if(resultAux.size() > 0) {
						result = result ++ Set(expExpression);
					}
				}
			} else {
				for(operand <- operands) {
					val resultChild = MorphSQLUtility.containedInPrefixes(operand, prefixes, allPrefixes);
					result = result ++ resultChild.toSet;
				}
			}		    
		  }
		}
		
		result;
	}
	
	def getSelectItemsByAliasJava(selectItems:java.util.Collection[ZSelectItem] , alias:String ) 
	: java.util.Collection[ZSelectItem] = {
		this.getSelectItemsByAlias(selectItems.toList, alias);
	}
	
	def getSelectItemsByAlias(selectItems:Iterable[ZSelectItem] , alias:String ) 
	: List[ZSelectItem] = {
		val result : List[ZSelectItem]= {
			val resultAux = selectItems.flatMap(selectItem => {
				val selectItemAlias = selectItem.getAlias();
				if(alias.equalsIgnoreCase(selectItemAlias)) {
					Some(selectItem);
				} else {
				  None
				}		  
			})
			resultAux.toList;
		}
		
		result;
	}

	def  getSelectItemsMapPrefixJava(selectItems:java.util.Collection[ZSelectItem], node:Node , pPrefix:String 
	    , dbType:String ) : java.util.Collection[ZSelectItem] = {
	  this.getSelectItemsMapPrefix(selectItems, node, pPrefix, dbType);
	}
	
	def  getSelectItemsMapPrefix(selectItems:Iterable[ZSelectItem], node:Node , pPrefix:String 
	    , dbType:String ) : Iterable[ZSelectItem] = {
		val varNamePrefixed = Constants.PREFIX_MAPPING_ID + node.getName();
		val prefix = {
			if(!pPrefix.endsWith(".")) {
				pPrefix + ".";
			} else {
			  pPrefix
			}		  
		}

		val  result:List[ZSelectItem] = {
			val resultAux = selectItems.flatMap(selectItem => {
				val alias = selectItem.getAlias();
				
				if(alias != null && !alias.equals("")) {
					if(varNamePrefixed.equals(alias)) {
	//					result.add(selectItem);
						
						val newSelectItem = {
							if(selectItem.isExpression()) {
								selectItem;
							} else {
								val selectItemColumnName = selectItem.getColumn();
								MorphSQLSelectItem.apply(selectItemColumnName, prefix, dbType);
							}					  
						}
	
						newSelectItem.setAlias(alias);
						Some(newSelectItem);
					} else {
					  None
					}
				} else {
					val selectItemString = MorphSQLUtility.printWithoutEnclosedCharacters(
					    selectItem.toString());
					
					if(varNamePrefixed.equals(selectItemString)) {
	//					result.add(selectItem);
						
						val newSelectItem = {
							if(selectItem.isExpression()) {
								selectItem;
							} else {
								val selectItemColumnName = selectItem.getColumn();
								MorphSQLSelectItem.apply(selectItemColumnName, prefix, dbType);
							}					  
						}
						Some(newSelectItem);
					} else {
					  None
					} 
				}
			})
			resultAux.toList;
		}		
		
		result;
	}
	
	def getValueWithoutAlias(selectItem:ZSelectItem) : String = {
		val selectItemAlias = selectItem.getAlias();
		val resultAux = {
			if(selectItemAlias != null && !selectItemAlias.equals("")) {
				selectItem.setAlias("");
				val selectItemString = selectItem.toString();
				selectItem.setAlias(selectItemAlias);
				selectItemString
			} else {
				selectItem.toString();
			}		  
		}

//		val result = selectItem match {
//		  case sqlSelectItem:MorphSQLSelectItem => {
//			val columnType = sqlSelectItem.columnType;
//			resultAux.replaceAll("::" + columnType, "");
//		  }
//		  case _ => {
//		    resultAux
//		  }
//		}
		
		resultAux;
	}

	def pushOrderByDownJava(oldOrderByCollection:List[ZOrderBy]
	, mapInnerAliasSelectItem:Map[String, ZSelectItem]) : List[ZOrderBy] = {
	  this.pushOrderByDown(oldOrderByCollection.toList, mapInnerAliasSelectItem.toMap);
	}
	
	def pushOrderByDown(oldOrderByCollection:List[ZOrderBy]
	, mapInnerAliasSelectItem:Map[String, ZSelectItem]) : List[ZOrderBy] = {
		val whereReplacement = mapInnerAliasSelectItem.keySet.map(alias => {
			val selectItem = mapInnerAliasSelectItem(alias);
			
			val aliasColumn = new ZConstant(alias, ZConstant.COLUMNNAME);
			
			val dbType = {
				selectItem match {
				  case morphSelectItem:MorphSQLSelectItem => {morphSelectItem.dbType}
				  case _ => {null}
				}
			}


			
			val zConstant = {
				if(selectItem.isExpression()) {
					val selectItemValue = selectItem.getExpression().toString();
					MorphSQLConstant.apply(selectItemValue, ZConstant.UNKNOWN, dbType, null);
				} else {
					val selectItemAlias = selectItem.getAlias();
					if(selectItemAlias != null && !selectItemAlias.equals("")) {
						MorphSQLConstant.apply(selectItemAlias, ZConstant.COLUMNNAME, dbType, null);
					} else {
						val selectItemTable = selectItem.getTable();
						val selectItemColumn = selectItem.getColumn();
						
						val selectItemValue = {
							if(selectItemTable != null && !selectItemTable.equals("")) {
								selectItemTable + "." + selectItemColumn;  
							} else {
								selectItemColumn; 
							}						  
						}

						MorphSQLConstant.apply(selectItemValue, ZConstant.COLUMNNAME, dbType, null);
					}
				}			  
			}

			(aliasColumn -> zConstant);
		}).toMap

		val newOrderByCollection = {
		    oldOrderByCollection.map(oldOrderBy => {
			val orderByExp = oldOrderBy.getExpression();
			val newOrderByExp = MorphSQLUtility.replaceExp(orderByExp, whereReplacement);
			val newOrderBy = new ZOrderBy(newOrderByExp);
			newOrderBy.setAscOrder(oldOrderBy.getAscOrder());
			newOrderBy;		    
		  })
		}
		
		newOrderByCollection
		//this.setOrderBy(newOrderByCollection);
	}
	
	def setDefaultAliasJava(selectItems:java.util.Collection[ZSelectItem] ) = {
	  this.setDefaultAlias(selectItems.toList);
	}
	
	def setDefaultAlias(selectItems:Iterable[ZSelectItem] ) = {
		for(selectItem <- selectItems) {
			val selectItemAlias = selectItem.getAlias();
			val alias = {
				if(selectItemAlias == null || selectItemAlias.equals("")) {
				  selectItem.getColumn();
				} else {
				  selectItemAlias
				}
			}
			
			if(alias != null && !alias.equals("")) {
				selectItem.setAlias(alias);
			}
		}
	}
	
	def printWithoutEnclosedCharacters(oldString:String, dbType:String) : String = {
	  val enclosedCharacter = Constants.getEnclosedCharacter(dbType);
	  val result = oldString.replaceAll(enclosedCharacter, "");
	  result;
	}
	
	def printWithoutEnclosedCharacters(oldString:String) : String = {
	  var result = oldString; 
	  for(enclosedCharacter <- Constants.DATABASE_ENCLOSED_CHARACTERS) {
	    result = result.replaceAll(enclosedCharacter, "");
	  }
	  result;
	}

	def buildEqualitySQLExpression() = {
	  
	}
	
	def getXMLDatatype(pColumnName:String, mapXMLDatatype:Map[String, String], dbType:String) : String = {
		val columnName = MorphSQLUtility.printWithoutEnclosedCharacters(pColumnName, dbType).replaceAll("\"", "");
		val mappedType = mapXMLDatatype.find(p => p._1.equalsIgnoreCase(columnName));
		val result = if(mappedType.isDefined) { mappedType.get._2 } else { null }
		result;
	}	

}