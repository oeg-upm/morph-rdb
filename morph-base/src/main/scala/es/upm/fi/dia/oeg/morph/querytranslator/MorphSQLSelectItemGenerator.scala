package es.upm.fi.dia.oeg.morph.querytranslator

import Zql.ZConstant
import Zql.ZSelectItem
import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Node
import java.util.Collection
import scala.collection.JavaConversions._
import scala.collection.mutable.LinkedHashSet
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem

class MorphSQLSelectItemGenerator(nameGenerator:NameGenerator, dbType:String) {
	val logger = Logger.getLogger("SelectItemGenerator");
	

	def generateSelectItems(nodes:Collection[Node], pPrefix:String, oldSelectItems:Collection[ZSelectItem]
	, useAlias:Boolean) : Collection[ZSelectItem] = {
		val prefix = {
			if(pPrefix != null && !pPrefix.endsWith(".")) {
				pPrefix + ".";
			} else {
			  pPrefix
			}		  
		}
		

		var result = new LinkedHashSet[ZSelectItem]();

		for(node <- nodes) {
			if(!node.isLiteral()) {
				val selectItems = this.generateSelectItem(node, prefix, oldSelectItems
				    , useAlias);
				result.addAll(selectItems);				
			}
		}

		return result;
	}
	
	def generateSelectItem(node:Node, pPrefix:String, oldSelectItems:Collection[ZSelectItem]
	,  useAlias:Boolean) : Collection[ZSelectItem] = {
		var result2 : List[ZSelectItem] = Nil;
		var result : List[MorphSQLSelectItem] = Nil;
		
		val prefix = {
			if(pPrefix != null && !pPrefix.endsWith(".")) {
				pPrefix + ".";
			} else {
			  pPrefix
			}		  
		}


		val nameSelectVar = nameGenerator.generateName(node);

		var newSelectItem:ZSelectItem = null;
		var newSelectItem2:MorphSQLSelectItem = null;
		
		if(node.isVariable() || node.isURI()) {
			if(oldSelectItems == null) {
				val inputColumnName = {
					if(prefix == null) {
						nameSelectVar;
					} else {
						prefix + nameSelectVar;	
					}
				}
				newSelectItem = new ZSelectItem(inputColumnName);
				newSelectItem2 = MorphSQLSelectItem(inputColumnName, null, this.dbType, null);
				
				if(useAlias) {
					newSelectItem.setAlias(nameSelectVar);
					newSelectItem2.setAlias(nameSelectVar);
				}
				result2 = result2 ::: List(newSelectItem);
				result = result ::: List(newSelectItem2);
			} else {
				val oldSelectItemsIterator = oldSelectItems.iterator();
				while(oldSelectItemsIterator.hasNext()) {
					val oldSelectItem = oldSelectItemsIterator.next(); 
					val oldAlias = oldSelectItem.getAlias();
					var oldTable:String = null;
					var selectItemName:String=null;
					if(oldAlias == null || oldAlias.equals("")) {
						selectItemName = oldSelectItem.getColumn();
						oldTable = oldSelectItem.getTable();
					} else {
						selectItemName = oldAlias; 
					}

					var newSelectItemAlias:String = null;
					if(selectItemName.equalsIgnoreCase(nameSelectVar)) {
						val inputColumnName = {
							if(prefix == null || prefix.equals("")) {
								if(oldTable == null || oldTable.equals("")) {
									selectItemName;
								} else {
								  null
								} 
							} else {
								prefix + selectItemName;
							}
						}

						if(inputColumnName != null) {
						  	newSelectItem = new ZSelectItem(inputColumnName);
						  	newSelectItem2 = MorphSQLSelectItem(inputColumnName, null, this.dbType, null);
						}
						
						if(node.isVariable()) {
							newSelectItemAlias = node.getName();	
						} else if(node.isURI()) {
							newSelectItemAlias = node.getURI();
						}
					} else if (selectItemName.contains(nameSelectVar + "_")) {
						val inputColumnName = {
							if(prefix == null || prefix.equals("")) {
								if(oldTable == null || oldTable.equals("")) {
									selectItemName;
								} else {
									oldTable + "." + selectItemName;
								}
							} else {
								prefix + selectItemName;
							}						  
						}
						
						newSelectItem = new ZSelectItem(inputColumnName);
						newSelectItem2 = MorphSQLSelectItem(inputColumnName, null, this.dbType, null);
						
						if(node.isVariable()) {
							newSelectItemAlias = node.getName();	
						} else if(node.isURI()) {
							newSelectItemAlias = node.getURI();
						}						
						newSelectItemAlias += selectItemName.replaceAll(nameSelectVar, "");
					} else {
						newSelectItem = null;
					}

					if(newSelectItem != null) {
						if(newSelectItemAlias != null && useAlias) {
							newSelectItem.setAlias(newSelectItemAlias);
							newSelectItem2.setAlias(newSelectItemAlias);
						}
						result2 = result2 ::: List(newSelectItem);
						result = result ::: List(newSelectItem2);
					}
				}	
			}
		} else if(node.isLiteral()){
			val literalValue = node.getLiteralValue();
			
			val constantValue = {
				if(prefix == null) {
					literalValue.toString();
				} else {
					prefix + literalValue.toString();
				}			  
			}

			val exp = {
				literalValue match {
				  case _:java.lang.String => {
				    new ZConstant(constantValue, ZConstant.STRING);
				  }
				  case _:java.lang.Double => {
				    new ZConstant(constantValue, ZConstant.NUMBER);
				  }
				  case _ => {
				    new ZConstant(constantValue, ZConstant.STRING);
				  }
				}			  
			}

			newSelectItem = new ZSelectItem();
			newSelectItem.setExpression(exp);
			newSelectItem2 = MorphSQLSelectItem(exp)
			if(useAlias) { 
				newSelectItem.setAlias(nameSelectVar); 
				newSelectItem2.setAlias(nameSelectVar);
			}

			result2 = result2 ::: List(newSelectItem);
			result = result ::: List(newSelectItem2);
		} else {
			logger .warn("unsupported node " + node.toString());
		}

		return result;
	}
}
