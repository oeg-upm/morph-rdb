package es.upm.fi.dia.oeg.morph.base

import Zql.ZExpression
import Zql.ZConstant
import Zql.ZSelectItem
import scala.collection.JavaConversions._
import java.util.Collection
import Zql.ZExp
import com.hp.hpl.jena.graph.Node
import es.upm.fi.dia.oeg.morph.querytranslator.NameGenerator
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLConstant
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem
import Zql.ZOrderBy
import scala.collection.mutable.LinkedHashMap

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
	
	def getSelectItemByAlias(alias:String , selectItems:Collection[ZSelectItem] , prefix:String ) 
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
	
	def addSelectItem(selectItems:Collection[ZSelectItem] , newSelectItem:ZSelectItem) = {
		val newSelectItemAlias = newSelectItem.getAlias();
		val oldSelectItem = this.getSelectItemByAlias(newSelectItemAlias, selectItems, null);
		if (!oldSelectItem.isDefined) {
			selectItems.add(newSelectItem);
		}
	}

	def addSelectItems(selectItems:Collection[ZSelectItem] , newSelectItems:Collection[ZSelectItem]) = {
		for(newSelectItem <- newSelectItems) {
			this.addSelectItem(selectItems, newSelectItem);
		}
	}
	
	def setTableName(selectItems:Collection[MorphSQLSelectItem] , tableName:String) = {
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
	
	def  combineExpresions(exps:Collection[_ <: ZExp] , logicalOperator:String ) : ZExpression = {
		val it = exps.iterator();
		
		val result = {
			if(exps.size() == 0) {
			  null
			} else if(exps.size() == 1) {
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
	
	def replaceExp(oldExp : ZExp, mapReplacement : java.util.Map[ZConstant, ZConstant])  : ZExp= {
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
	  
	

	def printSelectItems(selectItems : Collection[ZSelectItem] , distinct : Boolean ) = {
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

	def areAllConstants(exps:List[ZExp] ) = {
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

	def containedInPrefix(exp:ZExp, prefix:String) : Collection[ZExpression] = {
		val prefixes = List(prefix);
		val result = MorphSQLUtility.containedInPrefixes(exp, prefixes, true);
		result;
	}
		
	def containedInPrefixes(exp:ZExp , prefixes:Collection[String] , allPrefixes:Boolean ) : Collection[ZExpression] = {
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
	
	def getSelectItemsByAlias(selectItems:Collection[ZSelectItem] , alias:String ) 
	: Collection[ZSelectItem] = {
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

	def  getSelectItemsMapPrefix(selectItems:Collection[ZSelectItem], node:Node , pPrefix:String 
	    , dbType:String ) : Collection[ZSelectItem] = {
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
		val result = {
			if(selectItemAlias != null && !selectItemAlias.equals("")) {
				selectItem.setAlias("");
				val selectItemString = selectItem.toString();
				selectItem.setAlias(selectItemAlias);
				selectItemString
			} else {
				selectItem.toString();
			}		  
		}

		return result;
	}

	def pushOrderByDown(oldOrderByCollection:Collection[ZOrderBy]
	, mapInnerAliasSelectItem:java.util.Map[String, ZSelectItem]) : java.util.Vector[ZOrderBy] = {
		val whereReplacement : LinkedHashMap[ZConstant, ZConstant] = LinkedHashMap.empty;
		
		for(alias <- mapInnerAliasSelectItem.keySet()) {
			val selectItem = mapInnerAliasSelectItem.get(alias);
			
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
			

			whereReplacement += (aliasColumn -> zConstant);
		}

		val newOrderByCollection : java.util.Vector[ZOrderBy] = {
		  val newOrderByCollectionAux = {
		    oldOrderByCollection.map(oldOrderBy => {
			val orderByExp = oldOrderBy.getExpression();
			val newOrderByExp = MorphSQLUtility.replaceExp(orderByExp, whereReplacement);
			val newOrderBy = new ZOrderBy(newOrderByExp);
			newOrderBy.setAscOrder(oldOrderBy.getAscOrder());
			newOrderBy;		    
		  })
		  }
		  new java.util.Vector(newOrderByCollectionAux);
		}
		
		newOrderByCollection
		//this.setOrderBy(newOrderByCollection);
	}
	
	def setDefaultAlias(selectItems:Collection[ZSelectItem] ) = {
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
	
	def printWithoutEnclosedCharacters(oldString:String) : String = {
	  var result = oldString; 
	  for(enclosedCharacter <- Constants.DATABASE_ENCLOSED_CHARACTERS) {
	    result = result.replaceAll(enclosedCharacter, "");
	  }
	  result;
	  
	}
}